package org.postgresql.ssl;

import java.lang.reflect.Method;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.util.Arrays;

import javax.security.auth.callback.PasswordCallback;

/**
 * 
 * bouncycastle resolve primary key
 */
public class BouncyCastlePrivateKeyFactory implements PrivateKeyFactory {

    private static boolean bcEnbled = false;
    private static Class<?> asnSequece = null;
    private static Class<?> privateKeyInfo = null;
    private static Class<?> jcaPemConverter = null;
    private static Class<?> jceOpensslBuilder = null;
    private static Class<?> inputDecryptorProvider = null;
    private static Class<?> pkcs8EncryptedPkeyInfo = null;
    private static Class<?> encryptPrivateKeyInfo;
    
    private static void initBc() throws Exception {
      String bouncyCastle = "org.bouncycastle.jce.provider.BouncyCastleProvider";
      try {
        Class<?> bc = Class.forName(bouncyCastle);
        Object bcObj = bc.newInstance();
        Security.addProvider((Provider) bcObj);
        asnSequece = Class.forName("org.bouncycastle.asn1.ASN1Sequence");
        privateKeyInfo = Class.forName("org.bouncycastle.asn1.pkcs.PrivateKeyInfo");
        jcaPemConverter = Class.forName("org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter");
        jceOpensslBuilder = Class
                .forName("org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder");
        inputDecryptorProvider = Class.forName("org.bouncycastle.operator.InputDecryptorProvider");
        pkcs8EncryptedPkeyInfo = Class.forName("org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo");
        encryptPrivateKeyInfo = Class.forName("org.bouncycastle.asn1.pkcs.EncryptedPrivateKeyInfo");
        bcEnbled = true;
      } catch (ClassNotFoundException e) {
        bcEnbled = false;
        throw new Exception("Counld not find some class: " + e.getMessage());
      } catch (Exception e) {
        bcEnbled = false;
        throw new Exception("Counld not find init bouncycastle: " + e.getMessage());
      }
    }
    
    private PrivateKey getPrivateKeyByBouncycastle(byte[] data,  PasswordCallback pwdcb) throws Exception {
      try {
        if(!bcEnbled) {
          synchronized (BouncyCastlePrivateKeyFactory.class) {
            if(!bcEnbled) {
              initBc();
            }
          }
        }
        if (bcEnbled) {
          byte[] arr = Arrays.copyOf(data, data.length);
          Method getInstance = asnSequece.getDeclaredMethod("getInstance", Object.class);
          Object se = getInstance.invoke(null, arr);
          Method epkInfo = encryptPrivateKeyInfo.getDeclaredMethod("getInstance", Object.class);
          Object epkInfoObj = epkInfo.invoke(null, se);
          Object pe = pkcs8EncryptedPkeyInfo.getConstructor(encryptPrivateKeyInfo).newInstance(epkInfoObj);
          Object converter = jcaPemConverter.newInstance();
          Object builder = jceOpensslBuilder.newInstance();
          Method build = jceOpensslBuilder.getDeclaredMethod("build", char[].class);
          if (pwdcb == null) {
            return null;
          }
          Object inputDecrptProvider = build.invoke(builder, pwdcb.getPassword());
          Method decryptPrivateKeyInfo = pkcs8EncryptedPkeyInfo.getDeclaredMethod("decryptPrivateKeyInfo",
                  inputDecryptorProvider);
          Object keyInfo = decryptPrivateKeyInfo.invoke(pe, inputDecrptProvider);
          Method getPrivateKey = jcaPemConverter.getDeclaredMethod("getPrivateKey", privateKeyInfo);
          Object pk = getPrivateKey.invoke(converter, keyInfo);
          PrivateKey privateKey = (PrivateKey) pk;
          return privateKey;
        }
      } catch (Exception e) {
          throw new Exception("get private key by bouncycastle failed:" + e.getMessage());
      }
      return null;
    }

    @Override
    public PrivateKey getPrivateKeyFromEncryptedKey(byte[] data, PasswordCallback pwdcb) throws Exception {
      return getPrivateKeyByBouncycastle(data, pwdcb);
    }

}
