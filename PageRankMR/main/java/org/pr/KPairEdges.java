package org.pr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class KPairEdges {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new Error("One argument required:\n<k-nodes>");
        }
        int k = Integer.parseInt(args[0]);
        int numNodes = k * k;
        double initialPR = 1.0 / numNodes;

        String info = null;
        BufferedWriter output = null;
        try {
            File file = new File("input/graph.txt");
            output = new BufferedWriter(new FileWriter(file));
            for (int i = 1; i <= numNodes; i++) {
                String f = String.valueOf(i);
                String t;
                if (i % k == 0) {
                    t = String.valueOf(0);
                }
                else {
                    t = String.valueOf(i + 1);
                }
                String pr = String.valueOf(initialPR);
                info = f + "\t" + t + "\t" + pr + "\n";
                output.write(info);
            }
        } catch ( IOException e ) {
            e.printStackTrace();
        } finally {
            if ( output != null ) {
                output.close();
            }
        }
    }
}
