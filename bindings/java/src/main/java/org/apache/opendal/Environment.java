package org.apache.opendal;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

public enum Environment {
    INSTANCE;

    public static final String UNKNOWN = "<unknown>";
    private String classifier = UNKNOWN;
    private String projectVersion = UNKNOWN;

    static {
        ClassLoader classLoader = Environment.class.getClassLoader();
        try (InputStream is = classLoader.getResourceAsStream("bindings.properties")) {
            final Properties properties = new Properties();
            properties.load(is);
            INSTANCE.classifier = properties.getProperty("os.detected.classifier", UNKNOWN);
            INSTANCE.projectVersion = properties.getProperty("project.version", UNKNOWN);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot load environment properties file", e);
        }
    }

    /**
     * Returns the classifier of the compiled environment.
     *
     * @return The classifier of the compiled environment.
     */
    public static String getClassifier() {
        return INSTANCE.classifier;
    }

    /**
     * Returns the version of the code as String.
     *
     * @return The project version string.
     */
    public static String getVersion() {
        return INSTANCE.projectVersion;
    }

}
