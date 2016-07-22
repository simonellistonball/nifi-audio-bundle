package com.simonellistonball.nifi.audio;

import org.apache.nifi.components.AllowableValue;

public class AudioConstants {
    public static final AllowableValue VALUE_LITTLE_ENDIAN = new AllowableValue("LITTLE_ENDIAN", "Little");
    public static final AllowableValue VALUE_BIG_ENDIAN = new AllowableValue("BIG_ENDIAN", "Big");
}
