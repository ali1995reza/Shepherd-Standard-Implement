package shepherd.standard.datachannel.standard.iobatch.unpooled;
;
import shepherd.standard.config.ConfigChangeResult;
import shepherd.standard.datachannel.standard.iobatch.BatchFactory;
import shepherd.standard.datachannel.standard.iobatch.ReadBatch;
import shepherd.standard.datachannel.standard.iobatch.WriteBatch;
import shepherd.standard.datachannel.standard.iobatch.unpooled.r.UnPooledReadBatch;
import shepherd.standard.datachannel.standard.iobatch.unpooled.w.UnPooledWriteBatch;
import shepherd.api.config.ConfigurationChangeResult;
import shepherd.api.config.ConfigurationKey;
import shepherd.api.config.IConfiguration;




public class UnPooledBatchFactory implements BatchFactory {

    private final PoorBufferController DEFAULT =
            new PoorBufferController(true);

    private final BufferControllerWrapper controller =
            new BufferControllerWrapper(DEFAULT);

    @Override
    public void initialize(IConfiguration configuration) {

        configuration.createSubConfiguration(
                "IoBatch"
        ).defineConfiguration(
                IoBatchConfigs.BUFFER_SIZE_CONTROLLER,
                DEFAULT,
                this::approveChange
        );
    }

    private final ConfigurationChangeResult approveChange(IConfiguration config, ConfigurationKey confName, Object oldVal, Object newVal) {

        if(confName == IoBatchConfigs.BUFFER_SIZE_CONTROLLER)
        {
            newVal = newVal==null?DEFAULT:newVal;

            controller.setRef((BufferSizeController) newVal);

            return ConfigChangeResult.newSuccessResult(newVal);
        }


        return ConfigChangeResult.newFailResult("config not found");
    }


    @Override
    public WriteBatch createWriteBatch() {
        return new UnPooledWriteBatch(
                controller
        );
        //return new UnPooledWriteBatch();
    }

    @Override
    public ReadBatch createReadBatch() {
        return new UnPooledReadBatch(
                controller
        );
    }
}
