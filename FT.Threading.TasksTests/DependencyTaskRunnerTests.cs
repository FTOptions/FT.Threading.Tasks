using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Concurrent;
using FT.Threading.Tasks;

namespace DependencyScheduler.Tests
{
    [TestClass()]
    public class DependencyTaskRunnerTests
    {
        [TestMethod()]
        public void RunTasks_Cycles()
        {
            var runner = new DependencyTaskRunner();

            var cTask = runner.AddTask("C", () => Console.WriteLine("C running"), new string[] { "A" });

            var aTask = runner.AddTask("A", () => Console.WriteLine("A running"), new string[] { "C" });

            //var bTask = runner.AddTask("B", ()=> Console.WriteLine("B running"), new string[] { "A", "B" });


            var runTask = runner.RunTasks();

            bool rightExceptionCaught = false;

            try
            {
                runTask.Wait();

            }
            catch (AggregateException ex)
            {
                ex.Handle((x) =>
                {
                    if (x is TopologicalSorterExtentions.CycleFoundException)
                    {
                        rightExceptionCaught = true;

                        return true;
                    }
                    else
                        return false;

                });


            }


            Assert.AreEqual(true, rightExceptionCaught, "Cycle detection is not working right!");

        }


        [TestMethod()]
        public void RunTasks_Order_And_Wait()
        {
            var runner = new DependencyTaskRunner();

            ConcurrentQueue<string> tasksOrder = new ConcurrentQueue<string>();

            var cTask = runner.AddTask("C", () => { tasksOrder.Enqueue("C"); System.Threading.Thread.Sleep(1000); }, new string[] { "A" });

            var aTask = runner.AddTask("A", () => { tasksOrder.Enqueue("A"); System.Threading.Thread.Sleep(2000); }, null);

            var runTask = runner.RunTasks();

            runTask.Wait();

            Assert.AreEqual("A", tasksOrder.ElementAt(0));
            Assert.AreEqual("C", tasksOrder.ElementAt(1));


        }



        [TestMethod()]
        public void RunTasks_Reset()
        {
            var runner = new DependencyTaskRunner();

            ConcurrentQueue<string> tasksOrder = new ConcurrentQueue<string>();

            var cTask = runner.AddTask("C", () => { tasksOrder.Enqueue("C"); }, new string[] { "A" });

            var aTask = runner.AddTask("A", () => { tasksOrder.Enqueue("A"); }, null);

            var runTask = runner.RunTasks();

            runTask.Wait();

            Assert.AreEqual("A", tasksOrder.ElementAt(0));
            Assert.AreEqual("C", tasksOrder.ElementAt(1));

            runner.Reset();

            cTask = runner.AddTask("C", () => { tasksOrder.Enqueue("C"); }, new string[] { "A" });

            aTask = runner.AddTask("A", () => { tasksOrder.Enqueue("A"); }, null);

            runTask = runner.RunTasks();

            runTask.Wait();

            Assert.AreEqual("A", tasksOrder.ElementAt(0));
            Assert.AreEqual("C", tasksOrder.ElementAt(1));
            Assert.AreEqual("A", tasksOrder.ElementAt(2));
            Assert.AreEqual("C", tasksOrder.ElementAt(3));


        }

        [TestMethod()]
        [ExpectedException(typeof(InvalidOperationException))]
        public void RunTasks_Reset_Exception()
        {
            var runner = new DependencyTaskRunner();

            ConcurrentQueue<string> tasksOrder = new ConcurrentQueue<string>();

            var cTask = runner.AddTask("C", () => { tasksOrder.Enqueue("C"); }, new string[] { "A" });

            var aTask = runner.AddTask("A", () => { tasksOrder.Enqueue("A"); }, null);

            var runTask = runner.RunTasks();

            runTask.Wait();

            Assert.AreEqual("A", tasksOrder.ElementAt(0));
            Assert.AreEqual("C", tasksOrder.ElementAt(1));


            cTask = runner.AddTask("C", () => { tasksOrder.Enqueue("C"); }, new string[] { "A" });

            aTask = runner.AddTask("A", () => { tasksOrder.Enqueue("A"); }, null);

            runTask = runner.RunTasks();

            runTask.Wait();

            Assert.AreEqual("A", tasksOrder.ElementAt(0));
            Assert.AreEqual("C", tasksOrder.ElementAt(1));
            Assert.AreEqual("A", tasksOrder.ElementAt(2));
            Assert.AreEqual("C", tasksOrder.ElementAt(3));


        }

        [TestMethod()]
        public void RunTasks_Ignore_Missing_Deps()
        {
            var runner = new DependencyTaskRunner(ignoreMissingDependencies: true);

            ConcurrentQueue<string> tasksOrder = new ConcurrentQueue<string>();

            var cTask = runner.AddTask("C", () => { tasksOrder.Enqueue("C"); }, new string[] { "G" });

            var runTask = runner.RunTasks();

            runTask.Wait();

            Assert.IsTrue(tasksOrder.Contains("C"));
        }


        [TestMethod()]
        public void RunTasks_Cancellation()
        {
            var runner = new DependencyTaskRunner();

            ConcurrentQueue<string> tasksOrder = new ConcurrentQueue<string>();

            int cIters = 0;
            int aIters = 0;

            int bIters = 0;


            var cTask = runner.AddTask("C", (ct) =>
            {


                for (int i = 0; i < 10; i++)
                {
                    ct.ThrowIfCancellationRequested();

                    System.Threading.Thread.Sleep(100);

                    cIters++;
                }




            }, new string[] { "A" });


            var aTask = runner.AddTask("A", (ct) =>
            {

                for (int i = 0; i < 10; i++)
                {

                    ct.ThrowIfCancellationRequested();

                    System.Threading.Thread.Sleep(200);

                    aIters++;

                }

            }, null);


            var bTask = runner.AddTask("B", () =>
            {

                for (int i = 0; i < 10; i++)
                {

                    System.Threading.Thread.Sleep(50);

                    bIters++;

                }

            }, null);

            var runTask = runner.RunTasks();

            runner.RequestCancellationAfter(50);


            bool rightExceptionCaught = false;

            try
            {
                runTask.Wait();

            }
            catch (AggregateException ex)
            {
                var flat = ex.Flatten();

                flat.Handle((x) =>
                {
                    if (x is TaskCanceledException || x is OperationCanceledException)
                    {
                        rightExceptionCaught = true;


                    }

                    return true;
                });


            }




            Assert.AreEqual(true, rightExceptionCaught, "Cancellation not working right!");

            Assert.IsTrue(aIters < 10);
            bTask.Wait();
            Assert.IsTrue(bIters == 10); //this is one runs to completion

            Assert.IsTrue(cIters < 10);


        }




        class ProgressReceiver : IProgress<int>
        {
            public volatile int CurrentProgress;

            public void Report(int value)
            {
                CurrentProgress = value;

            }
        }

        [TestMethod()]
        public void RunTasks_PrpgressReporting()
        {
            var runner = new DependencyTaskRunner();

            ConcurrentQueue<string> tasksOrder = new ConcurrentQueue<string>();

            int cIters = 0;

            ProgressReceiver progC = new ProgressReceiver();


            var cTask = runner.AddTask("C", (ct, p) =>
            {

                for (int i = 0; i < 10; i++)
                {
                    p.Report(i);

                    ct.ThrowIfCancellationRequested();

                    System.Threading.Thread.Sleep(100);

                    cIters++;
                }

            }, progC, null);


            var runTask = runner.RunTasks();

            runTask.Wait();

            Assert.IsTrue(progC.CurrentProgress > 0);

        }


    }
}
