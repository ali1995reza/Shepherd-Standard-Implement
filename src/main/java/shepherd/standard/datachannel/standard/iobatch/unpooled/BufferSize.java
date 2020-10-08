package shepherd.standard.datachannel.standard.iobatch.unpooled;



public enum  BufferSize {

    B(1), KB(1024), MB(1024*1024);


    private final long mul;

    BufferSize(long mul) {
        this.mul = mul;
    }


    public int toByte(int i)
    {
        if(i<0)
            throw new IllegalStateException("size of buffer can not be negative");

        long cal = mul*i;

        if(cal>Integer.MAX_VALUE)
            throw new IllegalStateException("big size");


        return (int)cal;

    }

    public int toByte()
    {
        return toByte(1);
    }
}
