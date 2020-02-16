package com.anur;

import java.util.Random;
import java.util.TimerTask;
import com.anur.timewheel.TimedTask;
import com.anur.timewheel.Timer;

/**
 * Created by Anur IjuoKaruKas on 4/4/2019
 */
public class TestExecutor {

    public static void main(String[] args) {
        Timer instance = Timer.getInstance();
        Random random = new Random();

        for (int i = 0; i < 100000; i++) {
            int delayMs = random.nextInt(10000);

            instance.addTask(new TimedTask(delayMs, new TimerTask() {

                @Override
                public void run() {
                    System.out.println(delayMs);
                }
            }));
        }
    }
}
