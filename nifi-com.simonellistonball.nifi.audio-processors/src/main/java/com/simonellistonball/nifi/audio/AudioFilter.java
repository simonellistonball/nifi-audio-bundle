package com.simonellistonball.nifi.audio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;

import com.simonellistonball.nifi.audio.filters.LowPassFilter;
import com.simonellistonball.nifi.audio.utils.FilteredSoundStream;
import com.simonellistonball.nifi.audio.utils.SoundFilter;

@Tags({ "audio", "sound", "anomaly" })
@CapabilityDescription("Apply a filter to an audio clip")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "audio.sampleRate", description = "The bit rate of the audio"),
        @ReadsAttribute(attribute = "audio.sampleSizeInBits", description = "The bit depth audio"), @ReadsAttribute(attribute = "audio.bigEndian", description = "The endian of the audio format"),
        @ReadsAttribute(attribute = "audio.channels", description = "Number of audio channels"), @ReadsAttribute(attribute = "audio.encoding", description = "Encoding of the audio") })
@InputRequirement(Requirement.INPUT_REQUIRED)
public class AudioFilter extends AbstractAudioProcessor {

    @SuppressWarnings("serial")
    private static Set<String> filters = Collections.unmodifiableSet(new HashSet<String>() {
        {
            add("High Pass");
            add("Low Pass");
            add("Amplification");
        }
    });
    public static final PropertyDescriptor FILTER = new PropertyDescriptor.Builder().name("The filter to apply").description("This determines which filter we apply").allowableValues(filters)
            .required(true).build();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(10);
        if (flowFiles.isEmpty()) {
            return;
        }

        for (FlowFile flowFile : flowFiles) {
            AudioFormat format = getAudioFormat(flowFile);
            
            // choose the filter
            
            // inject the format
            
            // get appropriate parameters for the filter from the dynamic properties
            double lowPass = context.getProperty("lowpass").asDouble();
            
            SoundFilter filter = new LowPassFilter(format.getSampleRate(), lowPass);

            session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    // turn it all into an array of doubles and then do the filtering
                    long length = in.available();
                    AudioInputStream ais = new AudioInputStream(in, format, length);
                    FilteredSoundStream filteredSoundStream = new FilteredSoundStream(ais, filter);
                    IOUtils.copy(filteredSoundStream, out);
                }
            });
        }
    }

    
}
