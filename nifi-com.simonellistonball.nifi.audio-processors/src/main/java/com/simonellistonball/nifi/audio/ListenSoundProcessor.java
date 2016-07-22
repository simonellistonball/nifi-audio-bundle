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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
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
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "audio", "sound", "listen" })
@CapabilityDescription("Continuously listens to audio from a microphone while scheduled. On each run interval emits a flow file in WAV format containing sounds recorded.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "timestamp", description = "Beginning time of clip"), @WritesAttribute(attribute = "audio.sampleRate", description = "The bit rate of the audio"),
        @WritesAttribute(attribute = "audio.sampleSizeInBits", description = "The bit depth audio"), @WritesAttribute(attribute = "audio.bigEndian", description = "The endian of the audio format"),
        @WritesAttribute(attribute = "audio.channels", description = "Number of audio channels"), @WritesAttribute(attribute = "audio.encoding", description = "Encoding of the audio") })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
public class ListenSoundProcessor extends AbstractAudioProcessor {

    public static final int POLL_TIMEOUT_MS = 20;

    private final class AudioReaderThread implements Runnable {
        private boolean recording;
        private AudioFormat format;
        private int maxClipSize;

        private AudioReaderThread(AudioFormat format, int maxClipSize) {
            this.format = format;
            this.maxClipSize = maxClipSize;
        }

        @Override
        public void run() {
            try {
                DataLine.Info info = new DataLine.Info(TargetDataLine.class, format);

                TargetDataLine line = (TargetDataLine) AudioSystem.getLine(info);
                line.open(format, lineBufSize);

                AudioInputStream stream = new AudioInputStream(line);
                line.start();

                while (isRecording()) {
                    long timeStamp = System.currentTimeMillis();
                    try {
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        AudioInputStream ais = new AudioInputStream(stream, stream.getFormat(), maxClipSize);
                        // TODO need this to be in a lossless compressed format of some sort, since it will be written out to a flowfile
                        AudioSystem.write(ais, Type.WAVE, baos);
                        events.offer(new SoundClips(timeStamp, baos.toByteArray()), 100, TimeUnit.MILLISECONDS);
                    } catch (IOException e) {
                        getLogger().error("Cannot read sound", e);
                    } catch (InterruptedException e) {
                        getLogger().error("Cannot add sound to queue", e);
                    }
                    getLogger().debug("Adding sound event");
                }

                line.stop();
                line.close();
                line = null;
            } catch (LineUnavailableException e) {
                getLogger().error("Cannot create line-in", e);
            }

        }

        public boolean isRecording() {
            return recording;
        }

        public void setRecording(boolean recording) {
            this.recording = recording;
        }
    }

    public class SoundClips {
        public SoundClips(final long timeStamp, final byte[] data) {
            this.timeStamp = timeStamp;
            this.data = data;
        }

        private final long timeStamp;
        private final byte[] data;

        public long getTimeStamp() {
            return timeStamp;
        }

        public byte[] getData() {
            return data;
        }

    }

    public static final PropertyDescriptor CLIP_SIZE = new PropertyDescriptor.Builder().name("Clip Length").description("The length of sound clips to generate flow files for").required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();

    public static final Relationship SUCCESS = new Relationship.Builder().name("Success").description("Successfully recorded audio is passed to this relation").build();

    public static final PropertyDescriptor MAX_MESSAGE_QUEUE_SIZE = new PropertyDescriptor.Builder().name("Max Size of Message Queue")
            .description("The maximum size of the internal queue used to buffer messages being transferred from the underlying channel to the processor. "
                    + "Setting this value higher allows more messages to be buffered in memory during surges of incoming messages, but increases the total " + "memory used by the processor.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).defaultValue("10000").required(true).build();

    protected static final int lineBufSize = 16384;

    private List<PropertyDescriptor> descriptors = Collections.unmodifiableList(Arrays.asList(CLIP_SIZE, MAX_MESSAGE_QUEUE_SIZE, SAMPLE_BITS, SAMPLE_RATE, CHANNELS, SIGNED, BIG_ENDIAN));

    private Set<Relationship> relationships = Collections.singleton((SUCCESS));

    private LinkedBlockingQueue<SoundClips> events;

    private Map<String, String> attributes;

    protected volatile AudioReaderThread audioReaderThread;

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        getLogger().info("Scheduled, start recording");
        // setup a queue and listen for audio
        events = new LinkedBlockingQueue<SoundClips>(context.getProperty(MAX_MESSAGE_QUEUE_SIZE).asInteger());

        // setup audio format
        AudioFormat format = getAudioFormat(context);
        attributes = formatToAttributes(format);

        int maxClipSize = Float.valueOf(format.getSampleRate() * context.getProperty(CLIP_SIZE).asTimePeriod(TimeUnit.SECONDS).floatValue()).intValue();

        // start pulling off clips onto a blocking queue
        // need to move this to a background thread.
        audioReaderThread = new AudioReaderThread(format, maxClipSize);
        audioReaderThread.setRecording(true);

        final Thread readerThread = new Thread(audioReaderThread);
        readerThread.setName(getClass().getName() + " [" + getIdentifier() + "]");
        readerThread.setDaemon(true);
        readerThread.start();

    }

    @OnUnscheduled
    public void onUnscheduled(ProcessContext context) {
        getLogger().info("Unscheduled, stopping recording");
        audioReaderThread.setRecording(false);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // pull messages off the queue
        SoundClips message = getMessage(false, session);
        if (message == null) {
            return;
        }
        attributes.put("timestamp", String.valueOf(message.getTimeStamp()));

        FlowFile flowFile = session.create();
        flowFile = session.putAllAttributes(flowFile, attributes);
        flowFile = session.importFrom(new ByteArrayInputStream(message.getData()), flowFile);
        session.transfer(flowFile, SUCCESS);
    }

    /**
     * If longPoll is true, the queue will be polled with a short timeout, otherwise it will poll with no timeout which will return immediately.
     *
     * @param longPoll
     *            whether or not to poll the main queue with a small timeout
     * 
     * @return an event from one of the queue, or null if none are available
     */
    protected SoundClips getMessage(final boolean longPoll, final ProcessSession session) {
        SoundClips event = null;
        try {
            if (longPoll) {
                event = events.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } else {
                event = events.poll();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }

        if (event != null) {
            session.adjustCounter("Sound Clips Received", 1L, false);
        }

        return event;
    }

}
