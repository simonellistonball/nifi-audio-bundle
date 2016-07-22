package com.simonellistonball.nifi.audio;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioFormat.Encoding;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

public abstract class AbstractAudioProcessor extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder().name("Sucess").description("Successfully recorded audio is passed to this relation").build();
    public static final Relationship FAILURE = new Relationship.Builder().name("Failure").description("Failed conversions go here").build();
    public static final Relationship UNSUPPORTED_FORMAT = new Relationship.Builder().name("Unsupported format").description("Unsupported format requested").build();

    @SuppressWarnings("serial")
    protected Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<Relationship>() {
        {
            add(SUCCESS);
            add(FAILURE);
            add(UNSUPPORTED_FORMAT);
        }
    });

    protected static final PropertyDescriptor SAMPLE_RATE = new PropertyDescriptor.Builder().name("Sample Rate").description("The length of sound clips to generate flow files for")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).required(true).build();

    protected static final PropertyDescriptor SAMPLE_BITS = new PropertyDescriptor.Builder().name("Sample Bits").description("The length of sound clips to generate flow files for").required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR).allowableValues("8", "16", "20", "24", "32").build();

    protected static final PropertyDescriptor CHANNELS = new PropertyDescriptor.Builder().name("Channels").description("The length of sound clips to generate flow files for").required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();;

    protected static final PropertyDescriptor SIGNED = new PropertyDescriptor.Builder().name("Format: Signed").description("The length of sound clips to generate flow files for").required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

    protected static final PropertyDescriptor BIG_ENDIAN = new PropertyDescriptor.Builder().name("Format: Endian").description("The length of sound clips to generate flow files for").required(true)
            .allowableValues(AudioConstants.VALUE_BIG_ENDIAN, AudioConstants.VALUE_LITTLE_ENDIAN).build();

    protected AudioFormat getAudioFormat(FlowFile flowFile) {
        float sampleRate = Float.valueOf(flowFile.getAttribute("audio.sampleRate"));
        int sampleSizeInBits = Integer.valueOf(flowFile.getAttribute("audio.sampleSizeInBits"));
        int channels = Integer.valueOf(flowFile.getAttribute("audio.channels"));
        boolean bigEndian = flowFile.getAttribute("audio.bigEndian").equals(AudioConstants.VALUE_BIG_ENDIAN);
        String encoding = flowFile.getAttribute("audio.encoding");
        boolean signed = encoding.equals(Encoding.PCM_SIGNED.toString());
        AudioFormat format = new AudioFormat(sampleRate, sampleSizeInBits, channels, signed, bigEndian);
        return format;
    }

    AudioFormat getAudioFormat(final ProcessContext context) {
        float sampleRate = context.getProperty(SAMPLE_RATE).asFloat();
        int sampleSizeInBits = context.getProperty(SAMPLE_BITS).asInteger();
        int channels = context.getProperty(CHANNELS).asInteger();
        boolean signed = context.getProperty(SIGNED).asBoolean();
        boolean bigEndian = context.getProperty(BIG_ENDIAN).asBoolean();

        AudioFormat format = new AudioFormat(sampleRate, sampleSizeInBits, channels, signed, bigEndian);
        return format;
    }

    @SuppressWarnings("serial")
    protected Map<String, String> formatToAttributes(AudioFormat format) {
        return new HashMap<String, String>() {
            {
                put("audio.channels", String.valueOf(format.getChannels()));
                put("audio.sampleRate", String.valueOf(format.getSampleRate()));
                put("audio.sampleSizeInBits", String.valueOf(format.getSampleSizeInBits()));
                put("audio.encoding", String.valueOf(format.getEncoding()));
                put("audio.bigEndian", String.valueOf(format.isBigEndian()));
            }
        };
    }
    public Set<Relationship> getRelationships() {
        return relationships;
    }

}
