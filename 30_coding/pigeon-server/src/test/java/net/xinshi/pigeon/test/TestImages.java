package net.xinshi.pigeon.test;

/*import com.sun.image.codec.jpeg.JPEGCodec;
import com.sun.image.codec.jpeg.JPEGImageEncoder;
import com.sun.imageio.plugins.jpeg.JPEGImageWriter;*/

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-7-26
 * Time: 上午11:18
 * To change this template use File | Settings | File Templates.
 */
public class TestImages {


    /**
     * This method takes in an image as a byte array (currently supports GIF, JPG, PNG and possibly other formats) and
     * resizes it to have a width no greater than the pMaxWidth parameter in pixels. It converts the image to a standard
     * quality JPG and returns the byte array of that JPG image.
     *
     * @param pImageData the image data.
     * @param pMaxWidth  the max width in pixels, 0 means do not scale.
     * @return the resized JPG image.
     * @throws IOException if the iamge could not be manipulated correctly.
     */
/*

    public static byte[] resizeImageAsJPG(byte[] pImageData, int pMaxWidth) throws IOException {
        // Create an ImageIcon from the image data
        ImageIcon imageIcon = new ImageIcon(pImageData);
        int width = imageIcon.getIconWidth();
        int height = imageIcon.getIconHeight();

        // If the image is larger than the max width, we need to resize it
        if (pMaxWidth > 0 && width > pMaxWidth) {
            // Determine the shrink ratio
            double ratio = (double) pMaxWidth / imageIcon.getIconWidth();
            height = (int) (imageIcon.getIconHeight() * ratio);
            width = pMaxWidth;
        }

        // Create a new empty image buffer to "draw" the resized image into
        BufferedImage bufferedResizedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        // Create a Graphics object to do the "drawing"
        Graphics2D g2d = bufferedResizedImage.createGraphics();
        g2d.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
        // Draw the resized image
        g2d.drawImage(imageIcon.getImage(), 0, 0, width, height, null);
        g2d.dispose();
        // Now our buffered image is ready
        // Encode it as a JPEG
        ByteArrayOutputStream encoderOutputStream = new ByteArrayOutputStream();
        JPEGImageEncoder encoder = JPEGCodec.createJPEGEncoder(encoderOutputStream);
        encoder.encode(bufferedResizedImage);
        byte[] resizedImageByteArray = encoderOutputStream.toByteArray();
        return resizedImageByteArray;

    }
*/
/*

    public static void test(String spec, String filePath, String relatedfilePath) throws Exception {
        String extPart = "jpg";
        File f = new File(relatedfilePath);
        if (!f.exists() || f.length() == 0) {
            // System.out.println("doGenRelated ... " + f.getAbsolutePath() + " exists, skip " + spec);
            // CommonTools.writeString(os, "ok");
            // return;
            if (StringUtils.equalsIgnoreCase(extPart, "jpg") || StringUtils.equalsIgnoreCase(extPart, "jpeg") || StringUtils.equalsIgnoreCase(extPart, "gif") || StringUtils.equalsIgnoreCase(extPart, "png")) {
                FileInputStream fis = new FileInputStream(filePath);
                BufferedImage img = ImageIO.read(fis);
                fis.close();
                String[] xy = spec.split("X");
                int x = Integer.parseInt(xy[0]);
                int y = Integer.parseInt(xy[1]);
                BufferedImage resizedImage = Scalr.resize(img, x, y, null);
                ByteArrayOutputStream bs = new ByteArrayOutputStream();
                if (extPart.equalsIgnoreCase("jpg") || extPart.equalsIgnoreCase("jpeg")) {
                    try {
                        JPEGImageWriter imageWriter = (JPEGImageWriter) ImageIO.getImageWritersBySuffix("jpeg").next();
                        ImageOutputStream ios = ImageIO.createImageOutputStream(bs);
                        imageWriter.setOutput(ios);
                        JPEGImageWriteParam jpegParams = (JPEGImageWriteParam) imageWriter.getDefaultWriteParam();
                        jpegParams.setCompressionMode(JPEGImageWriteParam.MODE_EXPLICIT);
                        jpegParams.setCompressionQuality(0.95f);
                        IIOMetadata data = imageWriter.getDefaultImageMetadata(new ImageTypeSpecifier(resizedImage), jpegParams);
                        imageWriter.write(data, new IIOImage(resizedImage, null, null), jpegParams);
                        bs.close();
                        imageWriter.dispose();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return;
                    }
                } else {
                    ImageOutputStream imOut = ImageIO.createImageOutputStream(bs);
                    ImageIO.write(resizedImage, extPart, imOut);
                }
                FileOutputStream fos = new FileOutputStream(f.getAbsolutePath());
                fos.write(bs.toByteArray());
                fos.close();
            } else {
                System.out.println("not recognized image type");
                return;
            }
        }
    }
*/

}
