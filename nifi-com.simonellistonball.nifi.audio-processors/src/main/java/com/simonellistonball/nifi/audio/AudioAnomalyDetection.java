package com.simonellistonball.nifi.audio;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;

@Tags({ "audio", "sound", "anomaly" })
@CapabilityDescription("Detects anomalies in audio against a known pattern")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "audio.rate", description = "The bit rate of the audio") })
@WritesAttributes({ @WritesAttribute(attribute = "anomaly.probability", description = "The likelihood there is an anomaly in the clip") })
@InputRequirement(Requirement.INPUT_REQUIRED)
public class AudioAnomalyDetection extends AbstractProcessor {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(10);
        if (flowFiles.isEmpty()) {
            return;
        }
        for (FlowFile flowFile : flowFiles) {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    // turn it all into an array of doubles and then do the fft
                    
                }
            });
        }
    }

}
