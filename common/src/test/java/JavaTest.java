import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

public class JavaTest {
    private static SecretKeySpec generateMySQLAESKey(String key) throws Exception {
        final byte[] finalKey = new byte[16];
        int i = 0;
        for (byte b : key.getBytes(StandardCharsets.US_ASCII)) {
            finalKey[i++ % 16] ^= b;
        }
        return new SecretKeySpec(finalKey, "AES");
    }

    private static String decrypt(String source, String key) throws Exception {
        SecretKey AESKey = generateMySQLAESKey(key);
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.DECRYPT_MODE, AESKey);
        byte[] clearText = Hex.decodeHex(source.toCharArray());
        byte[] cipherTextBytes = cipher.doFinal(clearText);
        return new String(cipherTextBytes, StandardCharsets.UTF_8);
        //When i was a programmer
    }


    @Test
    public void test() throws Exception {
        System.out.println(decrypt("A716B7D06D86D786BD687259842C3C368B6E8D0E2FCD0C1BA40E4388118E69600254BA3D00866AEE669617DC789C319A08D1B79B9BF1B19517E59A992B5047978C6081F0484896E8D9512ABA20C05874C48DDB646F2807B9FFE33452F3FD2ED8", "text_column_5bnTFwYPcEBMSQ2jcLcKL1BZuucV2Pn"));
    }
}



