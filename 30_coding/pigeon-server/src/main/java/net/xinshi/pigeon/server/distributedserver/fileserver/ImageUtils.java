package net.xinshi.pigeon.server.distributedserver.fileserver;

import com.sun.imageio.plugins.jpeg.JPEGImageWriter;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.plugins.jpeg.JPEGImageWriteParam;
import javax.imageio.stream.ImageOutputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午4:32
 * To change this template use File | Settings | File Templates.
 */

public class ImageUtils {

    public static BufferedImage changeSize(Image origImage, int maxWidth, int maxHeight) {
        BufferedImage bufferedImage = null;
        try {
            if (origImage == null) return bufferedImage;
            int w = origImage.getWidth(null);
            int h = origImage.getHeight(null);
            if (w <= maxWidth && h <= maxHeight) {
            } else {
                if (w == h) {
                    if (maxWidth >= maxHeight) {
                        h = maxHeight;
                        w = h;
                    } else {
                        w = maxWidth;
                        h = w;
                    }
                } else if (w > h) {
                    float scale = (float) h / w;
                    w = maxWidth;
                    h = (int) (w * scale);
                    if (h > maxHeight) {
                        h = maxHeight;
                        w = (int) (h / scale);
                    }
                } else {
                    float scale = (float) w / h;
                    h = maxHeight;
                    w = (int) (h * scale);
                    if (w > maxWidth) {
                        w = maxWidth;
                        h = (int) (w / scale);
                    }
                }
            }
            origImage = origImage.getScaledInstance(w, h, Image.SCALE_SMOOTH);
            bufferedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
            Graphics2D g = bufferedImage.createGraphics();
            g.setBackground(Color.WHITE);
            g.fillRect(0, 0, w, h);
            bufferedImage.getGraphics().drawImage(origImage, 0, 0, w, h, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bufferedImage;
    }

    public static void changeSize(Image origImage, int maxWidth, int maxHeight, OutputStream os, String imageType) throws Exception {
        InputStream stream = null;
        try {
            BufferedImage bufferedImage = changeSize(origImage, maxWidth, maxHeight);
            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            if (imageType.equalsIgnoreCase("jpg") || imageType.equalsIgnoreCase("jpeg") || imageType.equalsIgnoreCase("gif")) {
                try {
                    JPEGImageWriter imageWriter = (JPEGImageWriter) ImageIO.getImageWritersBySuffix("jpeg").next();
                    ImageOutputStream ios = ImageIO.createImageOutputStream(bs);
                    imageWriter.setOutput(ios);
                    JPEGImageWriteParam jpegParams = (JPEGImageWriteParam) imageWriter.getDefaultWriteParam();
                    jpegParams.setCompressionMode(JPEGImageWriteParam.MODE_EXPLICIT);
                    jpegParams.setCompressionQuality(0.95f);
                    IIOMetadata data = imageWriter.getDefaultImageMetadata(new ImageTypeSpecifier(bufferedImage), jpegParams);
                    imageWriter.write(data, new IIOImage(bufferedImage, null, null), jpegParams);
                    bs.close();
                    imageWriter.dispose();
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            } else {
                ImageOutputStream imOut = ImageIO.createImageOutputStream(bs);
                ImageIO.write(bufferedImage, imageType, imOut);
            }
            stream = new ByteArrayInputStream(bs.toByteArray());
            byte[] buffer = new byte[2048];
            int n;
            while ((n = stream.read(buffer)) >= 0) {
                os.write(buffer, 0, n);
            }
            os.flush();
            os.close();
            os = null;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (os != null) os.close();
        }
    }

    public static BufferedImage addWaterMark(Image origImage, Image waterMarkImage, int width, int height) {
        BufferedImage bufferedImage = null;
        try {
            bufferedImage = changeSize(origImage, width, height);
            bufferedImage = addWaterMark(bufferedImage, waterMarkImage);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bufferedImage;
    }

    public static BufferedImage addWaterMark(Image origImage, Image waterMarkImage) {
        BufferedImage bufferedImage = null;
        try {
            int width = origImage.getWidth(null);
            int height = origImage.getHeight(null);
            bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
            Graphics2D g = bufferedImage.createGraphics();
            g.setBackground(Color.WHITE);
            g.fillRect(0, 0, width, height);
            g.drawImage(origImage, 0, 0, width, height, null);
            int wmWidth = waterMarkImage.getWidth(null);
            int wmHeight = waterMarkImage.getHeight(null);
            int w = 0;
            int h = 0;
            double scale = 1;
            if (wmWidth > wmHeight) {
                scale = ((double) wmHeight) / ((double) wmWidth);
            } else {
                scale = ((double) wmWidth) / ((double) wmHeight);
            }
            if (width > height) {
                if (wmWidth > wmHeight) {
                    if (wmWidth > width) {
                        wmWidth = width;
                    }
                    wmHeight = new Double(wmWidth * scale).intValue();
                } else {
                    if (wmHeight > height) {
                        wmHeight = height;
                    }
                    wmWidth = new Double(wmHeight * scale).intValue();
                }
            } else {
                if (wmHeight > wmWidth) {
                    if (wmHeight > height) {
                        wmHeight = height;
                    }
                    wmWidth = new Double(wmHeight * scale).intValue();
                } else {
                    if (wmWidth > width) {
                        wmWidth = width;
                    }
                    wmHeight = new Double(wmWidth * scale).intValue();
                }
            }
            if (width > wmWidth) {
                w = (width - wmWidth) / 2;
            }
            if (height > wmHeight) {
                h = (height - wmHeight) / 2;
            }
            waterMarkImage = waterMarkImage.getScaledInstance(wmWidth, wmHeight, Image.SCALE_SMOOTH);
            g.drawImage(waterMarkImage, w, h, wmWidth, wmHeight, null);
            g.dispose();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bufferedImage;
    }
}

