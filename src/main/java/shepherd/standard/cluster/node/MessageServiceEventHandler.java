package shepherd.standard.cluster.node;


import shepherd.standard.assertion.Assertion;
import shepherd.standard.config.ConfigChangeResult;
import shepherd.api.asynchronous.AsynchronousResultListener;
import shepherd.api.config.ConfigurationChangeResult;
import shepherd.api.config.ConfigurationKey;
import shepherd.api.config.IConfiguration;
import shepherd.utils.buffer.Utils;
import shepherd.utils.concurrency.threaddispatcher.SynchronizedDispatcher;
import shepherd.utils.concurrency.threaddispatcher.accumulator.syncdispatcher.AccumulatorIntegerKeySynchronizedDispatcher;
import shepherd.utils.concurrency.threaddispatcher.accumulator.syncdispatcher.AccumulatorSynchronizedDispatcher;
import shepherd.utils.concurrency.threaddispatcher.exceptions.DispatcherException;

import java.nio.ByteBuffer;

import static shepherd.standard.cluster.node.MessageServiceImpl.QUESTION;
import static shepherd.standard.cluster.node.MessageServiceImpl.QUESTION_RESPONSE;

class MessageServiceEventHandler {



    private SynchronizedDispatcher<Integer , MessageServiceEvent> syncDispatcher;
    private final MessageServiceManagerImpl serviceManager;
    private final MessageDispatcher publicDispatcher;
    private final Object _sync = new Object();
    private boolean started = false;
    private final StandardNode node;
    private final IConfiguration serviceConfig;



    MessageServiceEventHandler(MessageServiceManagerImpl serviceManager , StandardNode n , MessageDispatcher publicDispatcher)
    {
        Assertion.ifNull("provided service manager is null" , serviceManager);
        Assertion.ifNull("provided node is null" , n);
        Assertion.ifNull("public message dispatcher is null" , publicDispatcher );

        node = n;
        serviceConfig = node.configurations().createSubConfiguration("MessageService") ;

        serviceConfig.defineConfiguration(NodeConfigurations.NUMBER_OF_MESSAGE_EVENT_HANDLER_THREADS ,
                1 , this::approveConfigChange);

        this.serviceManager = serviceManager;
        this.publicDispatcher = publicDispatcher;
        syncDispatcher =
                new AccumulatorIntegerKeySynchronizedDispatcher<>(1 , this::handle);
    }


    boolean handleEvent(MessageServiceEvent event)
    {
        return syncDispatcher.tryDispatch(event.serviceId() , event);
    }



    void start()
    {
        synchronized (_sync) {
            initByConfigs();

            syncDispatcher.start();
        }
    }

    private void initByConfigs()
    {
        int numberOfHandlerThreads = serviceConfig.get(NodeConfigurations.NUMBER_OF_MESSAGE_EVENT_HANDLER_THREADS);
        syncDispatcher = new AccumulatorSynchronizedDispatcher<>(numberOfHandlerThreads , this::handle);
    }

    void stop()
    {
        synchronized (_sync) {

            syncDispatcher.terminateAndWaitToFinish();
        }
    }


    private void handle(int serviceId , MessageServiceEvent event)
    {

        MessageServiceImpl service = serviceManager.getService(serviceId);
        if(service!=null)
            event.handleBy(service);
        else if(event instanceof MessageEvent)
            handleUnSupportedService((MessageEvent)event);

        //todo something with this shit !
    }


    @Deprecated
    private void handle(MessageServiceEvent event)
    {
        MessageServiceImpl service = serviceManager.getService(event.serviceId());
        if(service!=null)
            event.handleBy(service);
        else if(event instanceof MessageEvent)
            handleUnSupportedService((MessageEvent)event);
    }




    private void handleUnSupportedService(MessageEvent event)
    {
        if(event.serviceHeader()!=QUESTION)
            return;


        int questionId = Utils.getInt(event.data());

        ByteBuffer resp = ByteBuffer.allocate(5);
        resp.putInt(questionId).
                put(
                        MessageServiceImpl.
                                ResponseErrorTypes.
                                DESTINATION_DOSE_NOT_SUPPORT_SERVICE
                ).
                flip();


        publicDispatcher.multicastData(event.serviceId() ,
                QUESTION_RESPONSE ,
                new ByteBuffer[]{resp} ,
                0 ,
                new int[]{event.sender().id()} ,
                AsynchronousResultListener.EMPTY);

    }





    private void setNumberOfHandlerThreads(int i) throws DispatcherException {


        syncDispatcher.setNumberOfHandlerThreads(i);
    }

    private ConfigurationChangeResult approveConfigChange(IConfiguration parent , ConfigurationKey confName , Object currentValue , Object value)
    {
        synchronized (_sync) {
            if (confName.equals(NodeConfigurations.NUMBER_OF_MESSAGE_EVENT_HANDLER_THREADS)) {
                if (value instanceof Integer) {
                    try {
                        int i = (Integer) value;

                        if (i <= 0)
                            return ConfigChangeResult.newFailResult("Number of handler threads can not be less than 1");

                        setNumberOfHandlerThreads(i);

                        return ConfigChangeResult.newSuccessResult(i);

                    } catch (Throwable e) {
                        throw new IllegalStateException(e);
                    }
                }
            }

            return ConfigChangeResult.newFailResult("Can not change config");
        }
    }


}
