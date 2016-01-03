package net.xinshi.pigeon.test;

import sun.security.pkcs.*;
import sun.security.util.DerValue;
import sun.security.x509.AlgorithmId;
import sun.security.x509.X500Name;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.X509Certificate;

public class FirmaSun {

    /*
   * * @param keyStore KeyStore Keystore that have got the certificate to sign
   * * @param alias Name of certificate to sign, into the keystore
   * * @param datos byte to sign
   * * @return String bytes that contains the PKCS7 created
   * */

    public static OutputStream firmar(KeyStore keyStore, String alias, byte[] datos) throws Exception {
        OutputStream salida = new ByteArrayOutputStream();
        String digestAlgorithm = "SHA1";
        String signingAlgorithm = "SHA1withRSA";
        PrivateKey priv = null;
        X509Certificate x509 = null;
        try {
            AlgorithmId[] digestAlgorithmIds = new AlgorithmId[]{AlgorithmId.get(digestAlgorithm)};
            MessageDigest md = MessageDigest.getInstance(digestAlgorithm);
            md.update(datos);
            byte[] digestedContent = md.digest();
            PKCS9Attribute[] authenticatedAttributeList = {new PKCS9Attribute(PKCS9Attribute.CONTENT_TYPE_OID, ContentInfo.DATA_OID), new PKCS9Attribute(PKCS9Attribute.SIGNING_TIME_OID, new java.util.Date()), new PKCS9Attribute(PKCS9Attribute.MESSAGE_DIGEST_OID, digestedContent)};
            PKCS9Attributes authenticatedAttributes = new PKCS9Attributes(authenticatedAttributeList);
            x509 = (X509Certificate) keyStore.getCertificateChain(alias)[0];
            priv = (PrivateKey) keyStore.getKey(alias, null);
            Signature signer = Signature.getInstance(signingAlgorithm);
            signer.initSign(priv);
            signer.update(authenticatedAttributes.getDerEncoding());
            byte[] signedAttributes = signer.sign();
            ContentInfo contentInfo = null;
            contentInfo = new ContentInfo(ContentInfo.DATA_OID, new DerValue(DerValue.tag_OctetString, datos));
            X509Certificate[] certificates = {x509};
            java.math.BigInteger serial = x509.getSerialNumber();
            SignerInfo si = new SignerInfo(new X500Name(x509.getIssuerDN().getName()), serial, AlgorithmId.getAlgorithmId(digestAlgorithm), authenticatedAttributes, new AlgorithmId(AlgorithmId.RSAEncryption_oid), signedAttributes, null);
            SignerInfo[] signerInfos = {si};
            PKCS7 p7 = new PKCS7(digestAlgorithmIds, contentInfo, certificates, signerInfos);
            p7.encodeSignedData(salida);              //only for test the verify method
            try {
                p7.verify();
            } catch (Exception e) {
                System.out.println("Error en validacion:" + e.getMessage());
                e.printStackTrace();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
        return salida;
    }
}