package shepherd.standard.config;

import shepherd.api.config.*;

final class UndefinableConfiguration implements IConfiguration {

    final static IConfiguration wrap(IConfiguration configuration)
    {
        return new UndefinableConfiguration(configuration);
    }

    private final IConfiguration configuration;

    private UndefinableConfiguration(IConfiguration configuration)
    {
        this.configuration = configuration;
    }

    @Override
    public <T> boolean defineConfiguration(ConfigurationKey<T> configName, T defaultValue, ConfigurationChangeController controller) {
        throw new IllegalStateException("can not define configuration");
    }

    @Override
    public <T> ConfigurationChangeResult<T> set(ConfigurationKey<T> confKey, T value) {
        return configuration.set(confKey, value);
    }

    @Override
    public <T> ConfigurationChangeResult<T> set(String config, T value) {
        return configuration.set(config, value);
    }

    @Override
    public <T> T get(ConfigurationKey<T> confKey) {
        return configuration.get(confKey);
    }

    @Override
    public <T> T get(ConfigurationKey<T> confKey, T defaultValue) {
        return configuration.get(confKey, defaultValue);
    }

    @Override
    public <T> T get(String configName) {
        return configuration.get(configName);
    }

    @Override
    public <T> T get(String configName, T defualtValue) {
        return configuration.get(configName , defualtValue);
    }

    @Override
    public <T> boolean remove(ConfigurationKey<T> confKey) {
        throw new IllegalStateException("can not remove configuration");
    }

    @Override
    public void addConfigChangeListener(ConfigChangeListener listener) {
        configuration.addConfigChangeListener(listener);
    }

    @Override
    public void removeConfigChangeListener(ConfigChangeListener listener) {
        configuration.removeConfigChangeListener(listener);

    }

    @Override
    public IConfiguration createSubConfiguration(String name) {
        return configuration.createSubConfiguration(name);
    }

    @Override
    public IConfiguration subConfiguration(String name) {
        return configuration.subConfiguration(name);
    }

    @Override
    public String name() {
        return configuration.name();
    }

    @Override
    public IConfiguration asUndefinableConfiguration() {
        return this;
    }

    @Override
    public IConfiguration parent() {
        return configuration.parent();
    }

    @Override
    public String toString() {
        return configuration.toString();
    }
}
