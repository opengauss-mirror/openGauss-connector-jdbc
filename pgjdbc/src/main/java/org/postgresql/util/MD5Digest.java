/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.util;
import org.postgresql.log.Logger;
import org.postgresql.ssl.BouncyCastlePrivateKeyFactory;
import org.postgresql.log.Log;
import org.postgresql.jdbc.ORConnectionHandler;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.spec.InvalidKeySpecException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Locale;

import javax.crypto.SecretKeyFactory;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec; 
import javax.crypto.spec.PBEKeySpec;



/**
 * MD5-based utility function to obfuscate passwords before network transmission.
 *
 * @author Jeremy Wohl
 */
public class MD5Digest {
    private static final int KEY_AGENT = 255;
    private static Log LOGGER = Logger.getLogger(MD5Digest.class.getName());

    private static final String SM3_PROVIDER_NAME = "BC";

  private MD5Digest() {
  }
  private static final String HMAC_SHA256_ALGORITHM = "HmacSHA256";

  /**
   * Encodes user/password/salt information in the following way: MD5(MD5(password + user) + salt).
   *
   * @param user The connecting user.
   * @param password The connecting user's password.
   * @param salt A four-salt sent by the server.
   * @return A 35-byte array, comprising the string "md5" and an MD5 digest.
   */
  public static byte[] encode(byte[] user, byte[] password, byte[] salt) {
    MessageDigest md;
    byte[] temp_digest;
    byte[] pass_digest;
    byte[] hex_digest = new byte[35];

    try {
      md = MessageDigest.getInstance("MD5");

      md.update(password);
      md.update(user);
      temp_digest = md.digest();

      bytesToHex(temp_digest, hex_digest, 0, 16);
      md.update(hex_digest, 0, 32);
      md.update(salt);
      pass_digest = md.digest();

      bytesToHex(pass_digest, hex_digest, 3, 16);
      hex_digest[0] = (byte) 'm';
      hex_digest[1] = (byte) 'd';
      hex_digest[2] = (byte) '5';
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("Unable to encode password with MD5", e);
    }

    return hex_digest;
  }

  /*
   * Turn 16-byte stream into a human-readable 32-byte hex string
   */
  private static void bytesToHex(byte[] bytes, byte[] hex, int offset, int length) {
    final char[] lookup =
        {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    int i;
    int c;
    int j;
    int pos = offset;

    for (i = 0; i < length; i++) {
      c = bytes[i] & 0xFF;
      j = c >> 4;
      hex[pos++] = (byte) lookup[j];
      j = (c & 0xF);
      hex[pos++] = (byte) lookup[j];
    }
  }
  
    public static byte[] SHA256_MD5encode(byte user[], byte password[], byte salt[]) {
        MessageDigest md, sha;
        byte[] temp_digest, pass_digest;
        byte[] hex_digest = new byte[70];
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(password);
            md.update(user);
            temp_digest = md.digest();
            bytesToHex(temp_digest, hex_digest, 0, 16);
            sha = MessageDigest.getInstance("SHA-256");
            sha.update(hex_digest, 0, 32);
            sha.update(salt);
            pass_digest = sha.digest();
            bytesToHex(pass_digest, hex_digest, 6, 32);
            hex_digest[0] = (byte) 's';
            hex_digest[1] = (byte) 'h';
            hex_digest[2] = (byte) 'a';
            hex_digest[3] = (byte) '2';
            hex_digest[4] = (byte) '5';
            hex_digest[5] = (byte) '6';
        } catch (Exception e) {
            LOGGER.info("SHA256_MD5encode failed. " + e.toString());
        }
        return hex_digest;
    }

    private static byte[] sha256(byte[] str) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            LOGGER.info("SHA256_MD5encode failed. ", e);
        }

        if (md == null) {
            return new byte[0];
        }

        md.update(str);
        return md.digest();
    }

    private static byte[] sm3(byte[] str) {
        MessageDigest md = null;
        try {
            Provider provider = null;
            if (Security.getProvider(SM3_PROVIDER_NAME) == null) {
                provider = BouncyCastlePrivateKeyFactory.initBouncyCastleProvider();
                Security.addProvider(provider);
                LOGGER.info("Load sm3 provider by JDBC Driver");
            } else {
                provider = Security.getProvider(SM3_PROVIDER_NAME);
            }
            md = MessageDigest.getInstance("SM3", provider);
        } catch (NoSuchAlgorithmException | SQLException exp) {
            LOGGER.info("SM3 encode failed.", exp);
        }
        if (md == null) {
            return new byte[0];
        }
        return md.digest(str);
    }

    private static String bytesToHexString(byte[] src) {
        StringBuilder stringBuilder = new StringBuilder("");
        if (src == null || src.length <= 0) {
            return null;
        }
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                stringBuilder.append(0);
            }
            stringBuilder.append(hv);
        }
        return stringBuilder.toString();
    }

    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }

    private static byte[] hexStringToBytes(String hexString) {
        if (hexString == null || hexString.equals("")) {
            return new byte[0];
        }
        hexString = hexString.toUpperCase(Locale.ENGLISH);
        int length = hexString.length() / 2;
        char[] hexChars = hexString.toCharArray();
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return d;
    }

    private static byte[] generateKFromPBKDF2(String password, String random64code, int server_iteration) {
        int iterations = server_iteration;
        char[] chars = password.toCharArray();
        byte[] random32code = hexStringToBytes(random64code);
        PBEKeySpec spec = new PBEKeySpec(chars, random32code, iterations, 32 * 8);
        SecretKeyFactory skf = null;
        try {
            skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
        } catch (NoSuchAlgorithmException e) {
            LOGGER.info("no algorithm: PBKDF2WithHmacSHA1. " + e.toString());
        }

        if (skf == null) return new byte[0];

        byte[] hash = null;
        try {
            hash = skf.generateSecret(spec).getEncoded();
        } catch (InvalidKeySpecException e) {
            LOGGER.info("mothod 'generateSecret' error. Invalid key. " + e.toString());
        }
        return hash;
    }

    private static byte[] generateKFromPBKDF2(String password, String random64code) {
        return generateKFromPBKDF2(password, random64code, 2048);
    }

    private static byte[] getKeyFromHmac(byte[] key, byte[] data) {
        SecretKeySpec signingKey = new SecretKeySpec(key, HMAC_SHA256_ALGORITHM);
        Mac mac = null;
        try {
            mac = Mac.getInstance(HMAC_SHA256_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            LOGGER.info("no algorithm: HMAC_SHA256_ALGORITHM. " + e.toString());
        }

        if (mac == null) {
            return new byte[0];
        }

        try {
            mac.init(signingKey);
        } catch (InvalidKeyException e) {
            LOGGER.info("method 'init' error. Invalid key. " + e.toString());
        }
        return mac.doFinal(data);
    }

    private static byte[] XOR_between_password(byte[] password1, byte[] password2, int length) {
        byte[] temp = new byte[length];
        for (int i = 0; i < length; i++) {
            temp[i] = (byte) (password1[i] ^ password2[i]);
        }
        return temp;
    }

    public static byte[] MD5_SHA256encode(String password, String random64code, byte salt[]) {
        MessageDigest md;
        byte[] temp_digest, pass_digest;
        byte[] hex_digest = new byte[35];
        try {
            StringBuilder stringBuilder = new StringBuilder("");
            byte[] K = MD5Digest.generateKFromPBKDF2(password, random64code);
            byte[] server_key = MD5Digest.getKeyFromHmac(K, "Sever Key".getBytes("UTF-8"));
            byte[] client_key = MD5Digest.getKeyFromHmac(K, "Client Key".getBytes("UTF-8"));
            byte[] stored_key = MD5Digest.sha256(client_key);
            stringBuilder.append(random64code);
            stringBuilder.append(MD5Digest.bytesToHexString(server_key));
            stringBuilder.append(MD5Digest.bytesToHexString(stored_key));
            String EncryptString = stringBuilder.toString();
            md = MessageDigest.getInstance("MD5");
            md.update(EncryptString.getBytes("UTF-8"));
            md.update(salt);
            pass_digest = md.digest();
            bytesToHex(pass_digest, hex_digest, 3, 16);
            hex_digest[0] = (byte) 'm';
            hex_digest[1] = (byte) 'd';
            hex_digest[2] = (byte) '5';
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            LOGGER.info("MD5_SHA256encode failed. ", e);
        } catch (Exception e) {
            LOGGER.info("MD5_SHA256encode failed. ", e);
        }
        return hex_digest;
    }

    public static byte[] RFC5802Algorithm(String password, String random64code, String token) {
        int server_iteration_350 = 2048;
        return RFC5802Algorithm(password, random64code, token, null, server_iteration_350, true);
    }

    public static byte[] RFC5802Algorithm(
            String password, String random64code, String token, String server_signature, int server_iteration, boolean isSha256) {
        byte[] hValue = null;
        byte[] result = null;
        try {
            byte[] K = generateKFromPBKDF2(password, random64code, server_iteration);
            byte[] server_key = getKeyFromHmac(K, "Sever Key".getBytes("UTF-8"));
            byte[] clientKey = getKeyFromHmac(K, "Client Key".getBytes("UTF-8"));
            byte[] storedKey = null;
            if (isSha256) {
                storedKey = sha256(clientKey);
            } else {
                storedKey = sm3(clientKey);
            }
            byte[] tokenbyte = hexStringToBytes(token);
            byte[] client_signature = getKeyFromHmac(server_key, tokenbyte);
            if (server_signature != null && !server_signature.equals(bytesToHexString(client_signature))) return new byte[0];
            byte[] hmac_result = getKeyFromHmac(storedKey, tokenbyte);
            hValue = XOR_between_password(hmac_result, clientKey, clientKey.length);
            result = new byte[hValue.length * 2];
            bytesToHex(hValue, result, 0, hValue.length);
        } catch (Exception e) {
            LOGGER.info("RFC5802Algorithm failed. " + e.toString());
        }
        return result;
    }

    public static byte[] RFC5802Algorithm(String password, String random64code, String token, int server_iteration) {
        return RFC5802Algorithm(password, random64code, token, null, server_iteration, true);
    }

    /**
     * encode password using sha256
     *
     * @param password password
     * @param scramble scramble
     * @param iteration iteration
     * @param conHandle ORConnectionHandler
     * @return encode data
     */
    public static byte[] sha256encode(String password, byte[] scramble, int iteration, ORConnectionHandler conHandle) {
        try {
            byte[] tokenByte = new byte[64];
            setBytes(scramble, 0, tokenByte, tokenByte.length);
            byte[] salt = new byte[16];
            setBytes(scramble, tokenByte.length, salt, salt.length);
            byte[] sha256Key = generateSha256KeyFromPBKDF2(password, salt, iteration);
            conHandle.setSha256Key(sha256Key);
            byte[] clientKey = getKeyFromHmac(sha256Key, "Zenith_Client_Key".getBytes("UTF-8"));
            byte[] storedKey = sha256(clientKey);
            byte[] hmacResult = getKeyFromHmac(storedKey, tokenByte);
            byte[] key = new byte[tokenByte.length + hmacResult.length];
            setBytes(tokenByte, 0, key, tokenByte.length);
            int[] arr = new int[hmacResult.length];
            for (int i = 0; i < hmacResult.length; i++) {
                arr[i] = clientKey[i] ^ hmacResult[i];
            }
            for (int i = 0; i < hmacResult.length; i++) {
                key[i + tokenByte.length] = (byte) (arr[i] & KEY_AGENT);
            }

            int encodeDataLen = key.length % 3 == 0 ? key.length / 3 * 4 : (key.length / 3 + 1) * 4;
            byte[] encodeBytes = new byte[encodeDataLen];
            encodeKey(key, encodeBytes);
            return encodeBytes;
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("SHA256_encode failed. ", e);
        } catch (Exception e) {
            LOGGER.error("SHA256_encode failed. ", e);
        }
        return new byte[0];
    }

    private static void setBytes(byte[] srcByte, int srcPos, byte[] destByte, int length) {
        for (int i = 0; i < length; i++) {
            destByte[i] = srcByte[i + srcPos];
        }
    }

    /**
     * verify sha256Key
     *
     * @param sha256Key sha256Key
     * @param scramble scramble
     * @param signingKey signingKey
     * @throws SQLException if a database access error occurs
     */
    public static void verifyKey(byte[] sha256Key, byte[] scramble, byte[] signingKey) throws SQLException {
        byte[] tokenByte = new byte[64];
        setBytes(scramble, 0, tokenByte, tokenByte.length);
        byte[] key = null;
        try {
            key = MD5Digest.getKeyFromHmac(sha256Key, "Zenith_Server_Key".getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new SQLException("generate server key failed");
        }
        byte[] targetSigningKey = MD5Digest.getKeyFromHmac(key, tokenByte);
        if (!Arrays.equals(targetSigningKey, signingKey)) {
            throw new SQLException("verify server key failed");
        }
    }

    private static byte[] generateSha256KeyFromPBKDF2(String password, byte[] salt, int server_iteration) {
        int iterations = server_iteration;
        char[] chars = password.toCharArray();
        PBEKeySpec spec = new PBEKeySpec(chars, salt, iterations, 32 * 8);
        SecretKeyFactory skf = null;
        try {
            skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("no algorithm: PBKDF2WithHmacSHA256. " + e.toString());
        }

        if (skf == null) {
            return new byte[0];
        }
        byte[] hash = null;
        try {
            hash = skf.generateSecret(spec).getEncoded();
        } catch (InvalidKeySpecException e) {
            LOGGER.error("mothod 'generateSecret' error. Invalid key. " + e.toString());
        }
        return hash;
    }

    private static void encodeKey(byte[] key, byte[] encodeBytes) {
        int p = 0;
        int i = 0;
        while (i + 2 < key.length) {
            int b0 = key[i++] & KEY_AGENT;
            byte v0 = (byte) (b0 / 4);
            encodeBytes[p++] = encodeByte(v0);
            int bt = (byte) (b0 * 16 & 0x3F);

            int b1 = key[i++] & KEY_AGENT;
            byte v1 = (byte) (bt | b1 / 16);
            encodeBytes[p++] = encodeByte(v1);
            bt = (byte) (b1 * 4 & 0x3F);

            int b2 = key[i] & KEY_AGENT;
            byte v2 = (byte) (bt | b2 / 64);
            encodeBytes[p++] = encodeByte(v2);
            encodeBytes[p++] = encodeByte(key[i++]);
        }
    }

    private static byte encodeByte(byte value) {
        byte b = (byte) (value & 0x3F);
        if (b > 62) {
            return 47;
        }
        if (b == 62) {
            return 43;
        }
        if (b >= 52) {
            return (byte) (b - 4);
        }
        if (b >= 26) {
            return (byte) (b + 71);
        }

        return (byte) (b + 65);
    }
}
