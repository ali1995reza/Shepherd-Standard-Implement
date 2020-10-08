package shepherd.standard.cluster.node;

import shepherd.api.config.ConfigurationKey;
import shepherd.api.logger.Logger;
import shepherd.api.logger.LoggerFactory;
import shepherd.api.message.MessageListener;
import shepherd.api.message.MessageSerializer;
import shepherd.api.message.MessageService;
import shepherd.api.message.MessageServiceManager;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

class MessageServiceManagerImpl implements MessageServiceManager {



    private final static class MessageServiceHolder {

        private final ConcurrentHashMap<Integer , MessageServiceImpl> mapServices;
        private final MessageServiceImpl[] arrayServices;

        private MessageServiceHolder()
        {
            mapServices = new ConcurrentHashMap<>();
            arrayServices = new MessageServiceImpl[100*1000+1]; //100001 size
        }

        private void assertIfAServiceWithIdAlreadyExist(int id , String message)
        {
            if(id<arrayServices.length)
            {
                //so we can check it here !
                if(arrayServices[id]!=null)
                    throw new IllegalStateException(message);
            }else if(mapServices.containsKey(id)){

                throw new IllegalStateException(message);
            }
        }


        MessageServiceImpl getService(int id)
        {
            if(id<arrayServices.length){
                return arrayServices[id];
            }

            return mapServices.get(id);
        }

        void addService(int id , MessageServiceImpl service)
        {
            if(id<arrayServices.length)
            {
                arrayServices[id] = service;
            }else
            {
                mapServices.put(id , service);
            }
        }

        boolean ensureAndRemoveService(int id , MessageService service)
        {
            if(service==null)
                throw new NullPointerException("provided service to remove is null");

            if(id<arrayServices.length)
            {
                if(arrayServices[id] == service)
                {
                    arrayServices[id] = null;
                    return true;
                }

                return false;
            }else
            {
                return mapServices.remove(id , service);
            }
        }

    }
















    private final Logger logger = LoggerFactory.factory().getLogger(this);

    private final MessageServiceHolder services;
    private final Object _sync = new Object();
    private Timer sharedTimer;
    private final MessageServiceEventHandler messageServiceEventHandler;
    private MessageDispatcher.Builder dispatcherBuilder;
    private StandardNode node;

    MessageServiceManagerImpl(StandardNode node)
    {
        services = new MessageServiceHolder();
        sharedTimer = node.sharedTimer();
        this.dispatcherBuilder = new MessageDispatcher.Builder(node , (byte)10 , node::sendValidate);
        this.node = node;
        messageServiceEventHandler =
                new MessageServiceEventHandler(this , node , dispatcherBuilder.build((byte)1));
    }




    @Override
    public <T> MessageService<T> registerService(int id, MessageSerializer<T> serializer, MessageListener<T> listener, Map<ConfigurationKey, Object> configs) {
        return createNewService(id, serializer, listener, configs);
    }

    @Override
    public <T> MessageService<T> registerService(int id, MessageSerializer<T> serializer, MessageListener<T> listener) {
        return registerService(id, serializer, listener , null);
    }

    @Override
    public MessageService<ByteBuffer[]> registerService(int id, MessageListener<ByteBuffer[]> listener, Map<ConfigurationKey, Object> configs) {
        return createNewService(id, listener, configs);
    }

    @Override
    public MessageService<ByteBuffer[]> registerService(int id, MessageListener<ByteBuffer[]> listener) {
        return registerService(id, listener , null);
    }

    @Override
    public void deregisterService(MessageService service) {
        removeService(service);
    }



    private final <T> MessageService<T> createNewService(int id
            , MessageSerializer<T> serializer
            , MessageListener<T> listener
            , Map<ConfigurationKey , Object> configs)
    {
        synchronized (_sync) {

            services.assertIfAServiceWithIdAlreadyExist( id  ,
                    "already a service with this id ["+id+"] registered");

            MessageServiceImpl<T> service =
                    new MessageServiceImpl<>(node
                            , dispatcherBuilder.build(id , parseConfigs(configs))
                            ,  id
                            , serializer
                            , listener
                            , sharedTimer
                            , adaptHandlerToSyncQueue(id));

            services.addService(id , service);

            logger.information("new service with id [{}] registered" , id);

            return service;
        }
    }


    private final MessageService<ByteBuffer[]> createNewService(int id
            , MessageListener<ByteBuffer[]> listener
            , Map<ConfigurationKey , Object> configs)
    {
        return createNewService(id , MessageServiceImpl.BYTE_BUFFER_ARRAY_SERIALIZER , listener , configs);
    }

    public MessageServiceImpl getService(int id)
    {
        return services.getService(id);
    }

    public void removeService(MessageService service)
    {
        synchronized (_sync)
        {
            if(!services.ensureAndRemoveService(service.id() , service))
            {
                throw new IllegalStateException("service not registered");
            }
        }
    }

    MessageServiceEventHandler messageServiceEventHandler()
    {
        return  messageServiceEventHandler;
    }

    boolean handleMessageEvent(MessageEvent event)
    {
        return messageServiceEventHandler.handleEvent(event);
    }

    private MessageServiceSyncQueue adaptHandlerToSyncQueue(int serviceId)
    {
        return new MessageServiceSyncQueue() {
            @Override
            public void enqueue(MessageServiceEvent messageEvent) {
                if(serviceId!=messageEvent.serviceId())
                {
                    throw new IllegalStateException("service id not equal to each other");
                }

                messageServiceEventHandler.handleEvent(messageEvent);
            }
        };
    }

    private final static byte parseConfigs(Map<ConfigurationKey , Object> confs)
    {
        if(confs==null)return 1;

        final Object priorityObject = confs.getOrDefault(MessageServiceConfiguration.PRIORITY , 1);

        if(priorityObject instanceof Byte)
        {
            return (byte)priorityObject;
        }else if(priorityObject instanceof Integer)
        {
            return (byte)(int)priorityObject;
        }else if(priorityObject instanceof Long)
        {
            return (byte)(long)priorityObject;
        }

        throw new IllegalArgumentException("priority must be a byte ");
        //todo save log of ignored options
    }

    public MessageDispatcher.Builder messageDispatcherBuilder() {
        return dispatcherBuilder;
    }

    void stopAllServices()
    {
        messageServiceEventHandler.stop();
    }
}
