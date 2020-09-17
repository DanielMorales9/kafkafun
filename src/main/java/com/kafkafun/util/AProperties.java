package com.kafkafun.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class AProperties {

    private String propertyFileName;

    public AProperties(String propertyFileName) {
        this.propertyFileName = propertyFileName;
    }

    public Properties getProperties() {
        Properties prop;

        try {
            InputStream input = new FileInputStream(String.format("src/main/resources/%s.properties", propertyFileName));
            prop = new Properties();

            // load a properties file
            prop.load(input);

            return prop;

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return getDefaultProperties();
    }

    public abstract Properties getDefaultProperties();

}

