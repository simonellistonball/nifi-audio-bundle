/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.simonellistonball.nifi.audio;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.sound.sampled.AudioFileFormat.Type;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.DataLine;
import javax.sound.sampled.LineUnavailableException;
import javax.sound.sampled.TargetDataLine;

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
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "audio", "sound", "listen" })
@CapabilityDescription("Continuously listens to audio from a microphone while scheduled. On each run interval emits a flow file in WAV format containing sounds recorded.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "timestamp", description = "Beginning time of clip"), @WritesAttribute(attribute = "audio.sampleRate", description = "The bit rate of the audio"),
        @WritesAttribute(attribute = "audio.sampleSizeInBits", description = "The bit depth audio"), @WritesAttribute(attribute = "audio.bigEndian", description = "The endian of the audio format"),
        @WritesAttribute(attribute = "audio.channels", description = "Number of audio channels"), @WritesAttribute(attribute = "audio.encoding", description = "Encoding of the audio") })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
public class ListenSoundClip extends AbstractAudioProcessor {

    public static final int POLL_TIMEOUT_MS = 20;

    public static final PropertyDescriptor CLIP_SIZE = new PropertyDescriptor.Builder().name("Clip Length").description("The length of sound clips to generate flow files for").required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();

    public static final Relationship SUCCESS = new Relationship.Builder().name("Success").description("Successfully recorded audio is passed to this relation").build();

    public static final PropertyDescriptor MAX_MESSAGE_QUEUE_SIZE = new PropertyDescriptor.Builder().name("Max Size of Message Queue")
            .description("The maximum size of the internal queue used to buffer messages being transferred from the underlying channel to the processor. "
                    + "Setting this value higher allows more messages to be buffered in memory during surges of incoming messages, but increases the total " + "memory used by the processor.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).defaultValue("10000").required(true).build();

    private static final PropertyDescriptor SAMPLE_RATE = new PropertyDescriptor.Builder().name("Sample Rate").description("The length of sound clips to generate flow files for")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).required(true).build();

    private static final PropertyDescriptor SAMPLE_BITS = new PropertyDescriptor.Builder().name("Sample Bits").description("The length of sound clips to generate flow files for").required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR).allowableValues("8", "16", "20", "24", "32").build();

    private static final PropertyDescriptor CHANNELS = new PropertyDescriptor.Builder().name("Channels").description("The length of sound clips to generate flow files for").required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();;

    private static final PropertyDescriptor SIGNED = new PropertyDescriptor.Builder().name("Format: Signed").description("The length of sound clips to generate flow files for").required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

    private static final PropertyDescriptor BIG_ENDIAN = new PropertyDescriptor.Builder().name("Format: Endian").description("The length of sound clips to generate flow files for").required(true)
            .allowableValues(AudioConstants.VALUE_BIG_ENDIAN, AudioConstants.VALUE_LITTLE_ENDIAN).build();

    protected static final int lineBufSize = 16384;

    private List<PropertyDescriptor> descriptors = Collections.unmodifiableList(Arrays.asList(CLIP_SIZE, MAX_MESSAGE_QUEUE_SIZE, SAMPLE_BITS, SAMPLE_RATE, CHANNELS, SIGNED, BIG_ENDIAN));

    private Set<Relationship> relationships = Collections.singleton((SUCCESS));

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    /**
     * Records a sound clip of the given length, blocking while running.
     * 
     * @param context
     * @param session
     * @throws ProcessException
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        AudioFormat format = getAudioFormat(context);
        Map<String, String> attributes = formatToAttributes(format);
        attributes.put("timestamp", String.valueOf(System.currentTimeMillis()));
        int maxClipSize = Float.valueOf(format.getSampleRate() * context.getProperty(CLIP_SIZE).asTimePeriod(TimeUnit.SECONDS).floatValue()).intValue();
        
        FlowFile flowFile = session.create();
        flowFile = session.putAllAttributes(flowFile, attributes);

        DataLine.Info info = new DataLine.Info(TargetDataLine.class, format);

        TargetDataLine line;
        try {
            line = (TargetDataLine) AudioSystem.getLine(info);
            line.open(format, lineBufSize);

            AudioInputStream stream = new AudioInputStream(line);
            line.start();

            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    AudioInputStream ais = new AudioInputStream(stream, stream.getFormat(), maxClipSize);
                    AudioSystem.write(ais, Type.WAVE, out);
                }

            });

            session.transfer(flowFile, SUCCESS);

        } catch (LineUnavailableException e) {
            context.yield();
        }

    }

}
