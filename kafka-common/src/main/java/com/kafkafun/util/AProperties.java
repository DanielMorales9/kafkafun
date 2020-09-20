package com.kafkafun.util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
            prop.load(new StringReader(resolveEnvVars(input)));

            return prop;

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return getDefaultProperties();
    }

    /*
     * Returns string with environment variable references expanded, e.g. $SOME_VAR or ${SOME_VAR}
     */
    private String resolveEnvVars(InputStream input) {

        Pattern p = Pattern.compile("\\$\\{(\\w+)\\}|\\$(\\w+)");

        return new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))
                .lines()
                .map(s -> {
                    Matcher m = p.matcher(s); // get a matcher object
                    StringBuffer sb = new StringBuffer();
                    while(m.find()){
                        String envVarName = null == m.group(1) ? m.group(2) : m.group(1);
                        String envVarValue = System.getenv(envVarName);
                        m.appendReplacement(sb, null == envVarValue ? "" : envVarValue);
                    }
                    m.appendTail(sb);
                    return sb.toString();
                }).collect(Collectors.joining("\n"));
    }

    abstract Properties getDefaultProperties();

}

