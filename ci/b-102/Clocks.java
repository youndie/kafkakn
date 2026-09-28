// B-102: the host's two clocks, side by side. Every 100 ms for SECONDS seconds: how far System.currentTimeMillis
// (the wall clock kafka-clients times poll with) moved against System.nanoTime (the monotonic clock the suite
// measures with). Prints the totals, the ratio, and every step where they parted by more than 50 ms.
//
//   java ci/b-102/Clocks.java 60
public class Clocks {
    public static void main(String[] args) throws Exception {
        int seconds = args.length > 0 ? Integer.parseInt(args[0]) : 60;
        long wall0 = System.currentTimeMillis(), mono0 = System.nanoTime();
        long wall = wall0, mono = mono0;
        int jumps = 0;
        for (int i = 0; i < seconds * 10; i++) {
            Thread.sleep(100);
            long w = System.currentTimeMillis(), m = System.nanoTime();
            long dw = w - wall, dm = (m - mono) / 1_000_000;
            if (Math.abs(dw - dm) > 50) {
                jumps++;
                System.out.printf("  step %d: wall %+d ms, monotonic %+d ms%n", i, dw, dm);
            }
            wall = w;
            mono = m;
        }
        long totalWall = wall - wall0, totalMono = (mono - mono0) / 1_000_000;
        System.out.printf("wall %d ms, monotonic %d ms, wall/monotonic %.3f, steps parted by >50 ms: %d%n",
                totalWall, totalMono, (double) totalWall / totalMono, jumps);
    }
}
