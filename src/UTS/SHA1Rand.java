package UTS;

import java.io.Serializable;

public class SHA1Rand implements Serializable {

    private static final long serialVersionUID = 1L;

    int w0 = 0;
    int w1 = 0;
    int w2 = 0;
    int w3 = 0;
    int w4 = 0; // 20 bytes
    //final RNG rng;
    final private int depth;

    public SHA1Rand() {
        this.depth = 0;
    }

    public SHA1Rand(int seed, int depth) {
        this.depth = depth;
        RNG.init(this, seed);
    }

    public SHA1Rand(SHA1Rand parent, int spawnNumber, int depth) {
        this.depth = depth;
        RNG.spawn(parent, this, spawnNumber);
    }

    public int getRand() {
        return RNG.rand(this);
    }

    public int getDepth() {
        return depth;
    }

//    public void setDepth(int depth) {
//        this.depth = depth;
//    }
}
