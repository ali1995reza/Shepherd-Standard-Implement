package shepherd.standard.utils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.UUID;

public class Hash {

    private static MessageDigest HashAlgorithm;
    private static SecureRandom RANDOM = new SecureRandom();

    static {
        try {
            HashAlgorithm = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    public final static String generateSecureHashString()
    {
        long currentMillis = System.currentTimeMillis();
        byte[] salt = RANDOM.generateSeed(20);
        UUID uuid = UUID.randomUUID();

        ByteBuffer buffer = ByteBuffer.allocate(300);
        buffer.putLong(currentMillis).put(salt);
        buffer.put(uuid.toString().getBytes(StandardCharsets.UTF_8));

        buffer.flip();

        synchronized (HashAlgorithm) {

            HashAlgorithm.update(buffer);
            byte[] hash = HashAlgorithm.digest();
            HashAlgorithm.reset();

            return String.format("%040x", new BigInteger(1, hash));
        }

    }
}
