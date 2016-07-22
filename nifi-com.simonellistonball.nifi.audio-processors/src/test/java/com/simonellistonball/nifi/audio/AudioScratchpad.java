package com.simonellistonball.nifi.audio;

import javax.sound.sampled.AudioFileFormat.Type;
import javax.sound.sampled.AudioSystem;

import org.junit.Test;

public class AudioScratchpad {

    @Test
    public void testAudioSystem() {
        Type[] audioFileTypes = AudioSystem.getAudioFileTypes();
        for (Type type: audioFileTypes) {
            System.out.println(type.getExtension());
        }
    }
    
    
}
