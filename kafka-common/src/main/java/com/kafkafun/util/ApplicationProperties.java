package com.kafkafun.util;

import java.util.Properties;

public class ApplicationProperties extends AProperties {

    public ApplicationProperties() {
        super("application");
    }

    public ApplicationProperties(String propertyFileName) {
        super(propertyFileName);
    }

    @Override
    public Properties getDefaultProperties() {
        return new Properties();
    }

}

