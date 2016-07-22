package com.simonellistonball.nifi.audio.filters;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;

import com.simonellistonball.nifi.audio.utils.SoundFilter;

/**
 * 
 * With some reference to http://stackoverflow.com/questions/4026648/how-to-implement-low-pass-filter-using-java
 * 
 * @author sball
 *
 */
public class LowPassFilter extends SoundFilter {

    public LowPassFilter(double frequency, double lowPass) {
        super();
        this.frequency = frequency;
        this.lowPass = lowPass;
    }

    /**
     * Frequency of input data (sample rate)
     */
    private double frequency;

    /**
     * The cutoff frequency
     */
    private double lowPass;

    @Override
    public void filter(byte[] samples, int offset, int length) {
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

        // build the classifier array, 2s are kept and 0s do not pass the filter
        double[] keepPoints = new double[frequencyDomain.length];
        keepPoints[0] = 1;
        for (int i = 1; i < frequencyDomain.length; i++) {
            if (frequencyDomain[i] < lowPass)
                keepPoints[i] = 2;
            else
                keepPoints[i] = 0;
        }

        // filter the fft
        for (int i = 0; i < fourierTransform.length; i++)
            fourierTransform[i] = fourierTransform[i].multiply((double) keepPoints[i]);

    }

}
