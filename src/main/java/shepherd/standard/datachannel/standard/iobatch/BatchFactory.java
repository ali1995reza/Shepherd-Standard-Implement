package shepherd.standard.datachannel.standard.iobatch;

import shepherd.api.config.IConfiguration;

public interface BatchFactory {

    void initialize(IConfiguration configuration);
    WriteBatch createWriteBatch();
    ReadBatch createReadBatch();
}
