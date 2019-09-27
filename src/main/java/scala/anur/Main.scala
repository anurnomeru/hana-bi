package scala.anur

import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

/**
 * Created by Anur IjuoKaruKas on 2019/9/25
 */
object Main extends App {

    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

    val pre = Future {
        println("task 1 start")
        Thread.sleep(3000)
        println("task 1 end")
        2
    }

    val cur = System.currentTimeMillis()

    val total = for {
        p <- pre
        abc <- {
            val A: Future[Int] = Future {
                println("task 2 start")
                Thread.sleep(1000)
                1
            }

            val B: Future[Int] = Future {
                println("task 3 start")
                Thread.sleep(4000)
                2
            }

            val C: Future[Int] = Future {
                println("task 4 start")
                Thread.sleep(3000)
                3
            }

            A.flatMap { a =>
                println("A Complete " + System.currentTimeMillis())
                B.flatMap { b =>
                    println("B Complete " + System.currentTimeMillis())
                    C.map { c =>
                        println("C Complete " + System.currentTimeMillis())
                        a + b + c
                    }
                }
            }

        }
    } yield {
        p + abc
    }

    total.onComplete {
        case Success(value) =>
            println(s"time cost is ${System.currentTimeMillis() - cur}")
            println(s"result is $value")
    }

    Thread.sleep(100000)
}
