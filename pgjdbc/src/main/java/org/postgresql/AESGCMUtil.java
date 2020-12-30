/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2019. All rights reserved.
 */

package org.postgresql;

import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;


import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

/**
 * This class provide encrypt and decrypt method.
 */
public class AESGCMUtil {
    private static Log LOGGER = Logger.getLogger(AESGCMUtil.class.getName());

    /* length of authentication tag */
    public static final int GCM_TAG_LENGTH = 16;

    /* length of initial vector */
    public static final int GCM_IV_LENGTH = 12;

    /* length of secret key */
    public static final int AES_KEY_SIZE = 128;

    /**
     * @description encrypt clear text
     * @param password: secret key
     * @param plaintext: clear text
     * @return ciphertext
     */
    public String encryptGCM(String password, String plaintext) {
        byte[] ciphertext = null;
        try {
            /* Construct a cipher */
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");

            /*
             * Generate random vectors using secure random numbers. Should not use the same IV encrypted data twice for two encryptions.
             */
            byte[] initVector = new byte[GCM_IV_LENGTH];
            new SecureRandom().nextBytes(initVector);

            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * java.lang.Byte.SIZE, initVector);

            /* Construct an AES private key */
            SecretKeySpec secretKeySpec;
            secretKeySpec = getSecretKeySpec(password);

            /* Initialize the cipher and set it to decrypt mode */
            cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, spec);

            /* Decryption and transcoding */
            byte[] encoded = plaintext.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ciphertext = new byte[initVector.length + cipher.getOutputSize(encoded.length)];

            for (int i = 0; i < initVector.length; i++) {
                ciphertext[i] = initVector[i];
            }

            /* Encrypt */
            cipher.doFinal(encoded, 0, encoded.length, ciphertext, initVector.length);
            return DatatypeConverter.printBase64Binary(ciphertext);

        } catch (InvalidKeyException | InvalidAlgorithmParameterException | NoSuchPaddingException
                | NoSuchAlgorithmException | UnsupportedEncodingException | ShortBufferException
                | IllegalBlockSizeException | BadPaddingException e) {
            LOGGER.info("encropt failed. except error", e);
        } catch (Exception e) {
            LOGGER.info("encropt failed.", e);
        }
        return "";
    }

    /**
     * @description decrypt ciphertext
     * @param password: secret key
     * @param encryptedtext: ciphertext
     * @return clear text
     */
    public String decryptGCM(String password, String encryptedtext) {
        byte[] decryptFrom = DatatypeConverter.parseBase64Binary(encryptedtext);
        byte[] plaintext = null;
        /* Initialization vector length */
        byte[] initVector = new byte[GCM_IV_LENGTH];

        try {
            Cipher cipher;
            cipher = Cipher.getInstance("AES/GCM/NoPadding");
            System.arraycopy(decryptFrom, 0, initVector, 0, GCM_IV_LENGTH);

            /* Instantiate GCM mode parameters */
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * java.lang.Byte.SIZE, initVector);

            SecretKeySpec secretKeySpec;
            secretKeySpec = getSecretKeySpec(password);
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, spec);
            plaintext = cipher.doFinal(decryptFrom, GCM_IV_LENGTH, decryptFrom.length - GCM_IV_LENGTH);
            return new String(plaintext, "UTF-8");
        } catch (NoSuchPaddingException | NoSuchAlgorithmException | IllegalBlockSizeException | BadPaddingException
                | UnsupportedEncodingException e) {
            LOGGER.info("decropt failed. except error", e);
        } catch (Exception e) {
            LOGGER.info("decropt failed. except error", e);
        }
        return "";
    }

    /**
     * @description construct an AES private key
     * @param password: secret key
     * @return AES private key
     * @throws NoSuchAlgorithmException
     * @throws UnsupportedEncodingException
     */
    public static SecretKeySpec getSecretKeySpec(String password)
            throws NoSuchAlgorithmException, UnsupportedEncodingException {
        /* Construct key generator, specify AES algorithm, case insensitive */
        KeyGenerator keygen = KeyGenerator.getInstance("AES");

        /**
         *  The secure random number generator, password.getBytes("UTF-8") is the seed. if the seed is the same, the generated security random number is the same.
         *  Must use this method. Otherwise secureRandom is not same in some Linux OS, and will catch "javax.crypto.AEADBadTagException: Tag mismatch". 
         */
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(password.getBytes("UTF-8"));

        /* Initialize the key generator to specify the key length */
        keygen.init(AES_KEY_SIZE, secureRandom);

        /* Generate the original key */
        SecretKey secretKey = keygen.generateKey();

        /* Get the key of the basic encoding format */
        byte[] keyByte = secretKey.getEncoded();

        /* Construct an AES private key */
        SecretKeySpec secretKeySpec = new SecretKeySpec(keyByte, "AES");

        return secretKeySpec;
    }
}
