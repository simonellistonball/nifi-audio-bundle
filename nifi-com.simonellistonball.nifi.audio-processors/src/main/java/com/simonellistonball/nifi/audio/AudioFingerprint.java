package com.simonellistonball.nifi.audio;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;

@Tags({ "audio", "sound", "anomaly" })
@CapabilityDescription("Detects anomalies in audio against a known pattern")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "audio.sampleRate", description = "The bit rate of the audio"),
        @ReadsAttribute(attribute = "audio.sampleSizeInBits", description = "The bit depth audio"), @ReadsAttribute(attribute = "audio.bigEndian", description = "The endian of the audio format"),
        @ReadsAttribute(attribute = "audio.channels", description = "Number of audio channels"), @ReadsAttribute(attribute = "audio.encoding", description = "Encoding of the audio") })
@WritesAttributes({ @WritesAttribute(attribute = "anomaly.probability", description = "The likelihood there is an anomaly in the clip") })
@InputRequirement(Requirement.INPUT_REQUIRED)
public class AudioFingerprint extends AbstractAudioProcessor {

    private int maxResults = 100;

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        List<FlowFile> list = session.get(maxResults);
        List<FlowFile> unsupported = new LinkedList<FlowFile>();

        for (FlowFile flowFile : list) {

            // some current restrictions
            final AudioFormat format = getAudioFormat(flowFile);

            if (format.getSampleSizeInBits() != 8) {
                unsupported.add(flowFile);
            } else {
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(InputStream in) throws IOException {
                        int length = in.available();

                        AudioInputStream ais = new AudioInputStream(in, format, length);
                        ByteArrayOutputStream baos = new ByteArrayOutputStream(length);

                        IOUtils.copy(ais, baos);
                        // Generates a fingerprint for the for audio, and then matches against stored fingerprints and routes appropriately

                        double[] frequencyDomain = frequencyDomain(baos.toByteArray(), format.getSampleRate());

                    }

                    private double[] frequencyDomain(byte[] samples, double frequency) {
                        int minPowerOf2 = 1;
                        while (minPowerOf2 < samples.length)
                            minPowerOf2 = 2 * minPowerOf2;

                        // pad with zeros
                        double[] padded = new double[minPowerOf2];
                        for (int i = 0; i < samples.length; i++)
                            padded[i] = samples[i];

                        FastFourierTransformer transformer = new FastFourierTransformer(DftNormalization.STANDARD);
                        Complex[] fourierTransform = transformer.transform(padded, TransformType.FORWARD);

                        // build the frequency domain array
                        double[] frequencyDomain = new double[fourierTransform.length];
                        for (int i = 0; i < frequencyDomain.length; i++)
                            frequencyDomain[i] = frequency * i / (double) fourierTransform.length;

                        return frequencyDomain;
                    }
                });
            }
        }

        session.transfer(unsupported, UNSUPPORTED_FORMAT);
    }

}
