package org.postgresql.ssl;

import java.security.PrivateKey;

import javax.security.auth.callback.PasswordCallback;
/**
 * 
 * Provide PrivateKey from EncryptedKey
 */
public interface PrivateKeyFactory {

    public PrivateKey getPrivateKeyFromEncryptedKey(byte[] data, PasswordCallback pwdcb)  throws Exception;
	
}
