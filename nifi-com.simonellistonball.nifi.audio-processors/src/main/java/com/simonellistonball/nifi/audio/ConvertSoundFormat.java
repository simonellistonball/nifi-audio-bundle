package com.simonellistonball.nifi.audio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.sound.sampled.AudioFileFormat.Type;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "audio", "sound", "convert" })
@CapabilityDescription("Convert sound format")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "audio.sampleRate", description = "The bit rate of the audio"),
        @ReadsAttribute(attribute = "audio.sampleSizeInBits", description = "The bit depth audio"), @ReadsAttribute(attribute = "audio.bigEndian", description = "The endian of the audio format"),
        @ReadsAttribute(attribute = "audio.channels", description = "Number of audio channels"), @ReadsAttribute(attribute = "audio.encoding", description = "Encoding of the audio") })
@WritesAttributes({ @WritesAttribute(attribute = "timestamp", description = "Beginning time of clip"), @WritesAttribute(attribute = "audio.sampleRate", description = "The bit rate of the audio"),
        @WritesAttribute(attribute = "audio.sampleSizeInBits", description = "The bit depth audio"), @WritesAttribute(attribute = "audio.bigEndian", description = "The endian of the audio format"),
        @WritesAttribute(attribute = "audio.channels", description = "Number of audio channels"), @WritesAttribute(attribute = "audio.encoding", description = "Encoding of the audio") })
@InputRequirement(Requirement.INPUT_REQUIRED)
public class ConvertSoundFormat extends AbstractAudioProcessor {

    public static final PropertyDescriptor OUTPUT_FORMAT = new PropertyDescriptor.Builder().name("Output format").displayName("The output format").description("Form of encoding for the file")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).build();

    private List<PropertyDescriptor> descriptors = Collections.unmodifiableList(Arrays.asList(OUTPUT_FORMAT, SAMPLE_BITS, SAMPLE_RATE, CHANNELS, SIGNED, BIG_ENDIAN));
    public List<PropertyDescriptor> getDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(10);
        if (flowFiles.isEmpty()) {
            return;
        }
        for (FlowFile flowFile : flowFiles) {
            AudioFormat inputFormat = getAudioFormat(flowFile);
            AudioFormat targetFormat = getAudioFormat(context);
            final Type fileType = getFileType(context.getProperty(OUTPUT_FORMAT).toString());
            if (fileType == null || !AudioSystem.isConversionSupported(targetFormat, inputFormat)) {
                session.transfer(flowFile, UNSUPPORTED_FORMAT);
                return;
            }
            
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    long length = in.available();
                    AudioInputStream ain = new AudioInputStream(in, inputFormat, length);
                    AudioInputStream convertedStream = AudioSystem.getAudioInputStream(targetFormat, ain);
                    AudioSystem.write(convertedStream, fileType, out);
                }
            });
            session.transfer(flowFile, SUCCESS);
        }
    }

    private Type getFileType(String type) {
        switch (type) {
        case "WAVE":
            return Type.WAVE;
        case "AU":
            return Type.AU;
        case "AIFF":
            return Type.AIFF;
        case "SND":
            return Type.SND;
        case "AIFC":
            return Type.AIFC;
        }
        return null;
    }

}
