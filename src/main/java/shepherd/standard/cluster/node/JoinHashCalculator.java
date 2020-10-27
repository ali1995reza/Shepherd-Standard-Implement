package shepherd.standard.cluster.node;



import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;


import static shepherd.standard.assertion.Assertion.*;

public final class JoinHashCalculator {

    private final static Comparator<Integer> SORTER = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1-o2;
        }
    };

    private final Map<Integer , String> hashes;
    private final MessageDigest digest;

    public JoinHashCalculator() {
        this.hashes = new HashMap<>();
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public JoinHashCalculator addHash(int i , String hash)
    {
        synchronized (hashes)
        {
            ifTrue("there is another hash for this id" , hashes.containsKey(i));
            ifTrue("id can not less than 1" , i<1);

            hashes.put(i , hash);
        }

        return this;
    }

    public JoinHashCalculator refresh()
    {
        hashes.clear();
        digest.reset();
        return this;
    }


    public String calculateHash()
    {
        synchronized (hashes)
        {
            List<Integer> keys = new ArrayList<>();
            for(int i:hashes.keySet())
            {
                keys.add(i);
            }

            keys.sort(SORTER);

            for(Integer id:keys)
            {
                String hash = hashes.get(id);
                digest.update(hash.getBytes());
            }

            byte[] hash = digest.digest();

            digest.reset();

            return String.format("%040x", new BigInteger(1, hash));
        }
    }

}
