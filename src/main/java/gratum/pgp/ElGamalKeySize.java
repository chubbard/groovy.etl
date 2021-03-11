package gratum.pgp;

import org.bouncycastle.jce.spec.ElGamalParameterSpec;

import java.math.BigInteger;

/**
 * Enumeration that carries predefined safe prime numbers specified from
 * https://www.ietf.org/rfc/rfc3526.txt.
 */
public enum ElGamalKeySize {

    // https://www.ietf.org/rfc/rfc3526.txt

    /**
     * This is a 4096 bit MODP group safe prime modulus
     * Prime number is: 2^4096 - 2^4032 - 1 + 2^64 * { [2^3996 pi] + 240904 }
     */
    BIT_4096(4096, "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1" +
            "29024E088A67CC74020BBEA63B139B22514A08798E3404DD" +
            "EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245" +
            "E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED" +
            "EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3D" +
            "C2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F" +
            "83655D23DCA3AD961C62F356208552BB9ED529077096966D" +
            "670C354E4ABC9804F1746C08CA18217C32905E462E36CE3B" +
            "E39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9" +
            "DE2BCBF6955817183995497CEA956AE515D2261898FA0510" +
            "15728E5A8AAAC42DAD33170D04507A33A85521ABDF1CBA64" +
            "ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7" +
            "ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6B" +
            "F12FFA06D98A0864D87602733EC86A64521F2B18177B200C" +
            "BBE117577A615D6C770988C0BAD946E208E24FA074E5AB31" +
            "43DB5BFCE0FD108E4B82D120A92108011A723C12A787E6D7" +
            "88719A10BDBA5B2699C327186AF4E23C1A946834B6150BDA" +
            "2583E9CA2AD44CE8DBBBC2DB04DE8EF92E8EFC141FBECAA6" +
            "287C59474E6BC05D99B2964FA090C3A2233BA186515BE7ED" +
            "1F612970CEE2D7AFB81BDD762170481CD0069127D5B05AA9" +
            "93B4EA988D8FDDC186FFB7DC90A6C08F4DF435C934063199" +
            "FFFFFFFFFFFFFFFF"),

    /**
     * This is a 3072 bit MODP group safe prime modulus
     * Prime is: 2^3072 - 2^3008 - 1 + 2^64 * { [2^2942 pi] + 1690314 }
     */
    BIT_3072(3072, "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1" +
            "29024E088A67CC74020BBEA63B139B22514A08798E3404DD" +
            "EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245" +
            "E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED" +
            "EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3D" +
            "C2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F" +
            "83655D23DCA3AD961C62F356208552BB9ED529077096966D" +
            "670C354E4ABC9804F1746C08CA18217C32905E462E36CE3B" +
            "E39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9" +
            "DE2BCBF6955817183995497CEA956AE515D2261898FA0510" +
            "15728E5A8AAAC42DAD33170D04507A33A85521ABDF1CBA64" +
            "ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7" +
            "ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6B" +
            "F12FFA06D98A0864D87602733EC86A64521F2B18177B200C" +
            "BBE117577A615D6C770988C0BAD946E208E24FA074E5AB31" +
            "43DB5BFCE0FD108E4B82D120A93AD2CAFFFFFFFFFFFFFFFF"),

    /**
     * This is a 2048 bit MODP group safe prime modulus
     * Prime is: 2^2048 - 2^1984 - 1 + 2^64 * { [2^1918 pi] + 124476 }
     */
    BIT_2048(2048, "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1" +
            "29024E088A67CC74020BBEA63B139B22514A08798E3404DD" +
            "EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245" +
            "E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED" +
            "EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3D" +
            "C2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F" +
            "83655D23DCA3AD961C62F356208552BB9ED529077096966D" +
            "670C354E4ABC9804F1746C08CA18217C32905E462E36CE3B" +
            "E39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9" +
            "DE2BCBF6955817183995497CEA956AE515D2261898FA0510" +
            "15728E5A8AACAA68FFFFFFFFFFFFFFFF"),

    /**
     * This is a 1536 bit MODP group safe prime modulus
     * Prime is: 2^1536 - 2^1472 - 1 + 2^64 * { [2^1406 pi] + 741804 }
     */
    BIT_1536(1536, "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1" +
            "29024E088A67CC74020BBEA63B139B22514A08798E3404DD" +
            "EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245" +
            "E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED" +
            "EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3D" +
            "C2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F" +
            "83655D23DCA3AD961C62F356208552BB9ED529077096966D" +
            "670C354E4ABC9804F1746C08CA237327FFFFFFFFFFFFFFFF");

    private int bitDepth;
    private String modulus;

    ElGamalKeySize(int bitDepth, String modulus) {
        this.bitDepth = bitDepth;
        this.modulus = modulus;
    }

    public BigInteger getModulus() {
        return new BigInteger(modulus, 16);
    }

    public int getBitDepth() {
        return bitDepth;
    }

    public ElGamalParameterSpec getElGamalParameterSpec() {
        return new ElGamalParameterSpec(getModulus(), getBaseGenerator());
    }

    public final BigInteger getBaseGenerator() {
        return new BigInteger("2", 16);
    }

    public static ElGamalKeySize getKeySize(int keySize) {
        for (ElGamalKeySize ks : values()) {
            if (ks.bitDepth == keySize) return ks;
        }

        StringBuilder buf = new StringBuilder();
        for (ElGamalKeySize ks : values()) {
            buf.append(ks.bitDepth > 0 ? "," : "").append(ks.bitDepth);
        }
        throw new IllegalArgumentException(String.format("Bad KeySize %d must be one of %s", keySize, buf.toString()));
    }
}
