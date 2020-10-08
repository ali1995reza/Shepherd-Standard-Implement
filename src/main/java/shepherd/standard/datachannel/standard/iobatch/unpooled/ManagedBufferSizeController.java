package shepherd.standard.datachannel.standard.iobatch.unpooled;

import static shepherd.standard.datachannel.standard.iobatch.unpooled.BufferSize.*;
import java.nio.ByteBuffer;

public class ManagedBufferSizeController implements BufferSizeController{


    public final static class Parameters implements BufferSizeController {


        private final int minimumSize;
        private final int maximumSize;
        private final boolean direct;
        private final boolean acceptExtendOffers;

        public Parameters(int minimumSize, int maximumSize, boolean direct, boolean acceptExtendOffers) {
            this.minimumSize = minimumSize;
            this.maximumSize = maximumSize;
            this.direct = direct;
            this.acceptExtendOffers = acceptExtendOffers;
        }

        @Override
        public String toString() {
            return "Parameters{" +
                    "minimumSize=" + minimumSize +
                    ", maximumSize=" + maximumSize +
                    ", direct=" + direct +
                    '}';
        }

        private final ByteBuffer allocate(int size)
        {
            if(direct)
                return ByteBuffer.allocateDirect(size);

            return ByteBuffer.allocate(size);
        }

        private boolean isInRange(int size)
        {
            return size>=minimumSize && size<=maximumSize;
        }

        @Override
        public ByteBuffer mustAllocate(int minSize, Section section) {
            if(minSize<minimumSize) minSize = minimumSize;

            return allocate(minSize);
        }



        @Override
        public ByteBuffer offerReallocate(int currentSize, int minSize, Section section) {
            if(minSize<minimumSize) minSize = minimumSize;

            if(currentSize>maximumSize && minSize>maximumSize)
            {
                return null;
            }

            if(isInRange(currentSize))
                return null;

            System.out.println("REALLOCATED : "+section);
            return allocate(minSize);
        }

        @Override
        public ByteBuffer offerExtend(int currentSize, int howMuch, Section section) {

            if(!acceptExtendOffers)return null;


            int total = currentSize+howMuch;

            System.out.println("OFFER EXTEND : "+total);
            if(isInRange(total))
                return allocate(total);

            return null;
        }

        @Override
        public ByteBuffer mustExtend(int currentSize, int howMuch, Section section) {

            int total = currentSize+howMuch;
            System.out.println("EXTEND");
            return allocate(total);
        }
    }



    private final static Parameters DEFAULT_PARAMETERS =
            new Parameters(KB.toByte(50) , MB.toByte(1) , true, true);


    private final Parameters[] parameters = new Parameters[7];

    public ManagedBufferSizeController()
    {
        for(int i=1;i<7;i++)
        {
            parameters[i] = DEFAULT_PARAMETERS;
        }
    }



    public ManagedBufferSizeController setParameters(
            Parameters param ,
            Section section
    ){
        if(param==null)param = DEFAULT_PARAMETERS;

        parameters[section.code] = param;

        return this;
    }

    public ManagedBufferSizeController setParameters(
            Parameters param ,
            Section ... sections
    ){
        if(param==null)param = DEFAULT_PARAMETERS;

        for(Section section:sections) {
            parameters[section.code] = param;
        }

        return this;
    }


    @Override
    public ByteBuffer mustAllocate(int miSize, Section section) {

        Parameters param = parameters[section.code];

        return param.mustAllocate(miSize, section);
    }

    @Override
    public ByteBuffer offerReallocate(int currentSize, int minSize, Section section) {
        Parameters param = parameters[section.code];

        return param.offerReallocate(currentSize, minSize , section);
    }

    @Override
    public ByteBuffer offerExtend(int currentSize, int howMuch, Section section) {
        Parameters param = parameters[section.code];

        return param.offerExtend(currentSize, howMuch , section);
    }

    @Override
    public ByteBuffer mustExtend(int currentSize, int howMuch, Section section) {
        Parameters param = parameters[section.code];

        return param.mustExtend(currentSize, howMuch , section);
    }

    @Override
    public String toString() {
        return "ManagedBufferSizeController";
    }
}
