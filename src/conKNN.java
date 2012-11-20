/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.*;
import java.util.Random;
import java.util.Scanner;
import java.util.StringTokenizer;

/**
 *
 * @author Del
 */
public class conKNN {

    private static String[][] modelData;
    private static String[][] observedData;
    private static boolean[] containNA;
    private static int gene = 18432;
    private static int experiment = 15;
    private static int k = 50;
    private static int Kth = 0;
    private static double closestDistance = 0;
    private static long startTime;
    private static long endTime;
    private static int degree;
    
    static void KNN(String[][] observationData, double tid) {
        double threadID = tid + 1;
         //System.out.println("Thread " + threadID + " is running...");
        Random t = new Random();
        int r, neighbor = 0;
        String[] lineContainsNA = new String[experiment];
        String[] lineRandom = new String[experiment];

        for (int i = (int) ((gene/degree)*tid); i < ((gene/degree)*threadID); i++) {
            for (int j = 0; j < experiment; j++) {
                //System.out.println("observation["+i+"]"+"["+j+"]" + observationData[i][j]);
                while ("NA".equals(observationData[i][j]) && neighbor != k) {
                    r = t.nextInt(gene);
                  //  System.out.println("observation["+i+"]"+"["+j+"]" + observationData[i][j]+"=NA"+" Thus, random row index = " + r);
                  //System.out.println("contain[" + r + "]..." + containNA[r]);
                    if (containNA[r] == false) {

                        for (int l = 0; l < experiment; l++) {
                            if (j != l && !"NA".equals(observationData[i][l])) {
                                lineContainsNA[l] = observationData[i][l];
                                lineRandom[l] = observationData[r][l];

                            } else {
                                lineContainsNA[l] = "0";
                                lineRandom[l] = "0";
                            }
                        }

                        if (closestDistance > EuclideanDistance(lineContainsNA, lineRandom)) {
                            closestDistance = EuclideanDistance(lineContainsNA, lineRandom);
                           // System.out.println("closest distance is " + closestDistance);
                            Kth = r;
                        }
                        neighbor++;
                        if (neighbor == k) {
                            
                            observationData[i][j] = observationData[Kth][j];
                           // System.out.println("NA = " + observationData[i][j]);
                        }
                    }
                }
                neighbor = 0;
            }
        }
    }

    static double NormalizedRMSE(String[][] mData, String[][] oData) {
        double sumTop = 0;
        double sumBottom = 0;
        double distance;
        for (int i = 0; i < gene; i++) {
            for (int j = 0; j < experiment; j++) {
                distance = Double.parseDouble(mData[i][j]) - Double.parseDouble(oData[i][j]);
                sumTop = sumTop + (distance * distance);
                sumBottom = sumBottom + Double.parseDouble(mData[i][j]);
            }
        }
        return Math.sqrt(sumTop / sumBottom);
    }

    static double EuclideanDistance(String[] pointA, String[] pointB) {

        double sum = 0;
        double distance;
        for (int i = 0; i < pointA.length; i++) {

            distance = Double.parseDouble(pointA[i]) - Double.parseDouble(pointB[i]);
            sum = sum + (distance * distance);
        }
        return Math.sqrt(sum);

    }

    public static void main(String[] args) throws FileNotFoundException, IOException {

        String line1, line2;
        StringTokenizer element1, element2;
        int counter = 0;
        double NormalizedRMSE = 0;
        modelData = new String[gene][experiment];
        observedData = new String[gene][experiment];
        containNA = new boolean[gene];


        System.out.println("Welcome to the concurrent KNN");
        Scanner input = new Scanner(System.in);
        System.out.print("Enter number of thread = ");
        degree = input.nextInt();

        System.out.println("concurrent KNN method started...");
        System.out.println("data initializing...");
        FileInputStream fstream1 = new FileInputStream("/Users/Del/Desktop/data.txt");
        FileInputStream fstream2 = new FileInputStream("/Users/Del/Desktop/data-5.txt");
        // get primitive type data
        DataInputStream dstream1 = new DataInputStream(fstream1);
        DataInputStream dstream2 = new DataInputStream(fstream2);
        //get character type data
        BufferedReader buffer1 = new BufferedReader(new InputStreamReader(dstream1));
        BufferedReader buffer2 = new BufferedReader(new InputStreamReader(dstream2));
        //recording the thread start time
        startTime = System.currentTimeMillis();
        //Read the file line by line
        while ((line1 = buffer1.readLine()) != null && (line2 = buffer2.readLine()) != null) {
            // Print the content on the console
            //System.out.println(line1);
            //System.out.println(line2);


            containNA[counter] = false;
            element1 = new StringTokenizer(line1);
            element2 = new StringTokenizer(line2);
            if (line2.contains("NA") == true) {
                containNA[counter] = line2.contains("NA");
            }

            for (int i = 0; i < experiment; i++) {
                modelData[counter][i] =
                        element1.nextToken();
                observedData[counter][i] =
                        element2.nextToken();
             //  System.out.print(observedData[counter][i] + " ");
            }

            //System.out.println(containNA[counter]);
            counter++;
        }
        //Close the input stream

        fstream1.close();
        dstream1.close();
        fstream2.close();
        dstream2.close();
        //call KNN function with missing data set     

        Thread[] thread = new Thread[degree];

        for (int i = 0; i < 1; i++) {
            // preparing each thread 
            for (int j = 0; j < thread.length; j++) {
                final double tId = j;
                thread[j] = new Thread(new Runnable() {
                    public void run() {
                            KNN(observedData,tId);
                    }
                });
            }

            // starting each thread
            for (int n = 0; n < thread.length; n++) {
                thread[n].start();
            }

            // waiting for each thread to exit
            for (int m = 0; m < thread.length; m++) {
                try {
                    thread[m].join();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                }
            }

        }

        for (int i = 0; i < gene; i++) {
            for (int j = 0; j < experiment; j++) {
               // System.out.print(observedData[i][j] + " ");
            }
            //System.out.println();
        }
        //normalize error 
        NormalizedRMSE = NormalizedRMSE(modelData, observedData);
        System.out.println("Normalized Root Mean Square Error = " + NormalizedRMSE);
        //recording the end time of the program
        endTime = System.currentTimeMillis();

        System.out.println("...Concurrent KNN operation is done with " + (endTime - startTime) + " milliseconds");

    }
}
