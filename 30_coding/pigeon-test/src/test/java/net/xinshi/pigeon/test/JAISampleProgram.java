package net.xinshi.pigeon.test;
/*

import com.sun.media.jai.codec.FileSeekableStream;
import com.sun.media.jai.codec.SeekableStream;
import javax.media.jai.Interpolation;
import javax.media.jai.JAI;
import javax.media.jai.OpImage;
import javax.media.jai.RenderedOp;
import java.awt.*;
import java.awt.image.renderable.ParameterBlock;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
*/

import java.io.IOException;

public class JAISampleProgram {

      /**
     * This method takes in an image as a byte array (currently supports GIF, JPG, PNG and
     * possibly other formats) and
     * resizes it to have a width no greater than the pMaxWidth parameter in pixels.
     * It converts the image to a standard
     * quality JPG and returns the byte array of that JPG image.
     *
     * @param pImageData the image data.
     * @param pMaxWidth  the max width in pixels, 0 means do not scale.
     * @return the resized JPG image.
     * @throws IOException if the image could not be manipulated correctly.
     */

    public static byte[] resizeImageAsJPG(byte[] pImageData, int pMaxWidth) throws IOException {

       /* InputStream imageInputStream = new ByteArrayInputStream(pImageData);
        SeekableStream seekableImageStream = SeekableStream.wrapInputStream(imageInputStream, true);
        RenderedOp originalImage = JAI.create("stream", seekableImageStream);
        ((OpImage) originalImage.getRendering()).setTileCache(null);
        int origImageWidth = originalImage.getWidth();
        double scale = 1.0;
        if (pMaxWidth > 0 && origImageWidth > pMaxWidth) {
            scale = (double) pMaxWidth / originalImage.getWidth();
        }
        ParameterBlock params = new ParameterBlock();
        params.addSource(originalImage); // The source image
        params.add(scale); // The xScale
        params.add(scale); // The yScale
        params.add(0.0); // The x translation
        params.add(0.0); // The y translation
        Interpolation interp = Interpolation.getInstance(Interpolation.INTERP_BILINEAR);
        params.add(interp);
        RenderingHints qualityHints = new RenderingHints(RenderingHints.KEY_RENDERING,
                RenderingHints.VALUE_RENDER_QUALITY);
        RenderedOp resizedImage = JAI.create("SubsampleAverage", params, qualityHints);
        ByteArrayOutputStream encoderOutputStream = new ByteArrayOutputStream();
        JAI.create("encode", resizedImage, encoderOutputStream, "JPEG", null);
        byte[] resizedImageByteArray = encoderOutputStream.toByteArray();
        return resizedImageByteArray;*/

        return null;
    }


    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java JAISampleProgram input image_filename");
            System.exit(-1);
        }
                /*
        FileSeekableStream stream = null;
        try {
            stream = new FileSeekableStream(args[0]);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }

        RenderedOp image1 = JAI.create("stream", stream);
        Interpolation interp = Interpolation.getInstance(Interpolation.INTERP_BILINEAR);

        ParameterBlock params = new ParameterBlock();
        params.addSource(image1);
        params.add(2.0F);
        params.add(2.0F);
        params.add(0.0F);
        params.add(0.0F);
        params.add(interp);

        RenderedOp image2 = JAI.create("scale", params);

        int width = image2.getWidth();
        int height = image2.getHeight();

        ScrollingImagePanel panel = new ScrollingImagePanel(image2, width, height);

        Frame window = new Frame("JAI Sample Program");
        window.add(panel);
        window.pack();
        window.show();*/

    }

}