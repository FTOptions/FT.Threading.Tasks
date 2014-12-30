using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FT.Threading.Tasks
{
    /// <summary>
    /// allows to schedule and execute titled tasks that have dependencies
    /// </summary>
    public class DependencyTaskRunner : IDisposable
    {
        private class TaskHolder
        {
            public Task ScheduledTask { get; set; }
            public string[] Dependencies { get; set; }
            public TaskHolder[] ResolvedDependencies { get; set; }


        }

        private TaskScheduler _ts;

        private Dictionary<string, TaskHolder> _scheduledTasks = new Dictionary<string, TaskHolder>();

        private Task _lastRunTask;

        private bool _ignoreMissingDeps;

        private CancellationTokenSource _ctsSource;
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ignoreMissingDependencies">does not throw exception when task dependency is not satisfied</param>
        /// <param name="ts">if scheduler that limits concurrency is desired see http://msdn.microsoft.com/en-us/library/ee789351%28v=vs.110%29.aspx </param>
        public DependencyTaskRunner(bool ignoreMissingDependencies = false, TaskScheduler ts = null)
        {
            _ts = ts;
            _ignoreMissingDeps = ignoreMissingDependencies;
            _ctsSource = new CancellationTokenSource();

        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool TryGetTaskByTitle(string title, out Task task)
        {
            TaskHolder tTemp = null;

            bool res = _scheduledTasks.TryGetValue(title, out tTemp);

            if (res)
            {
                task = tTemp.ScheduledTask;

                return true;
            }
            else
            {
                task = null;

                return false;

            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Task AddTask(string title, Action taskMethod, string[] taskDeps)
        {
            if (_lastRunTask != null)
                throw new InvalidOperationException("This scheduler has already been run. if you want to re-use this instance, call reset method.");

            if (string.IsNullOrWhiteSpace(title))
                throw new ArgumentNullException();

            if (_scheduledTasks.ContainsKey(title))
                throw new ArgumentOutOfRangeException("title", "task with following name already exists: " + title);

            var task = new Task(taskMethod, _ctsSource.Token);

            _scheduledTasks.Add(title, new TaskHolder() { ScheduledTask = task, Dependencies = taskDeps });

            return task;

        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Task AddTask(string title, Action<CancellationToken> taskMethod, string[] taskDeps)
        {
            if (_lastRunTask != null)
                throw new InvalidOperationException("This scheduler has already been run. if you want to re-use this instance, call reset method.");

            if (string.IsNullOrWhiteSpace(title))
                throw new ArgumentNullException();

            if (_scheduledTasks.ContainsKey(title))
                throw new ArgumentOutOfRangeException("title", "task with following name already exists: " + title);

            Action taskMethodRaw = () => taskMethod(_ctsSource.Token);

            var task = new Task(taskMethodRaw, _ctsSource.Token);

            _scheduledTasks.Add(title, new TaskHolder() { ScheduledTask = task, Dependencies = taskDeps });

            return task;

        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Task AddTask(string title, Action<CancellationToken, IProgress<int>> taskMethod, IProgress<int> progress, string[] taskDeps)
        {
            if (_lastRunTask != null)
                throw new InvalidOperationException("This scheduler has already been run. if you want to re-use this instance, call reset method.");

            if (string.IsNullOrWhiteSpace(title))
                throw new ArgumentNullException();

            if (_scheduledTasks.ContainsKey(title))
                throw new ArgumentOutOfRangeException("title", "task with following name already exists: " + title);

            Action taskMethodRaw = () => taskMethod(_ctsSource.Token, progress);

            var task = new Task(taskMethodRaw, _ctsSource.Token);

            _scheduledTasks.Add(title, new TaskHolder() { ScheduledTask = task, Dependencies = taskDeps });

            return task;

        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Task<T> AddTask<T>(string title, Func<T> taskMethod, string[] taskDeps)
        {
            if (_lastRunTask != null)
                throw new InvalidOperationException("This scheduler has already been run. if you want to re-use this instance, call reset method.");


            if (string.IsNullOrWhiteSpace(title))
                throw new ArgumentNullException();

            if (_scheduledTasks.ContainsKey(title))
                throw new ArgumentOutOfRangeException("title", "task with following name already exists: " + title);

            Task<T> task = new Task<T>(taskMethod, _ctsSource.Token);

            _scheduledTasks.Add(title, new TaskHolder() { ScheduledTask = task, Dependencies = taskDeps });

            return task;

        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Task<T> AddTask<T>(string title, Func<CancellationToken, T> taskMethod, string[] taskDeps)
        {
            if (_lastRunTask != null)
                throw new InvalidOperationException("This scheduler has already been run. if you want to re-use this instance, call reset method.");

            if (string.IsNullOrWhiteSpace(title))
                throw new ArgumentNullException();

            if (_scheduledTasks.ContainsKey(title))
                throw new ArgumentOutOfRangeException("title", "task with following name already exists: " + title);


            Func<T> taskMethodRaw = () => taskMethod(_ctsSource.Token);

            Task<T> task = new Task<T>(taskMethodRaw, _ctsSource.Token);

            _scheduledTasks.Add(title, new TaskHolder() { ScheduledTask = task, Dependencies = taskDeps });

            return task;

        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Task<T> AddTask<T>(string title, Func<CancellationToken, IProgress<int>, T> taskMethod, IProgress<int> progress, string[] taskDeps)
        {
            if (_lastRunTask != null)
                throw new InvalidOperationException("This scheduler has already been run. if you want to re-use this instance, call reset method.");

            if (string.IsNullOrWhiteSpace(title))
                throw new ArgumentNullException();

            if (_scheduledTasks.ContainsKey(title))
                throw new ArgumentOutOfRangeException("title", "task with following name already exists: " + title);


            Func<T> taskMethodRaw = () => taskMethod(_ctsSource.Token, progress);

            Task<T> task = new Task<T>(taskMethodRaw, _ctsSource.Token);

            _scheduledTasks.Add(title, new TaskHolder() { ScheduledTask = task, Dependencies = taskDeps });

            return task;

        }

        /// <summary>
        /// builds a DAG, makes sure that no cycles exist in dependency tree and executes tasks in a manner as parallel as possible while satisfying all dependencies
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Task RunTasks()
        {
            if (_lastRunTask != null)
                throw new InvalidOperationException("This scheduler has already been run. if you want to re-use this instance, call reset method.");

            _lastRunTask = new Task(() =>
            {

                var taskList = new List<TaskHolder>();

                //resolving dependencies in terms of task holders
                foreach (var kvp in _scheduledTasks)
                {
                    var t = kvp.Value;

                    if (t.Dependencies != null && t.Dependencies.Length > 0)
                    {
                        List<TaskHolder> deps = new List<TaskHolder>();

                        foreach (var dep in t.Dependencies)
                        {
                            TaskHolder tTemp = null;

                            if (!_scheduledTasks.TryGetValue(dep, out tTemp))
                            {
                                if (!_ignoreMissingDeps)
                                    throw new ApplicationException("Dependency resolution for Task[" + kvp.Key + "]: Could not locate missing dependent task named: " + dep);
                            }
                            else
                                deps.Add(tTemp);
                        }

                        t.ResolvedDependencies = deps.ToArray();
                    }

                    taskList.Add(t);
                }

                //make sure we got no cycles in our dependency graph
                var sortedTaskList = taskList.SortTopological((t) =>
                {
                    return t.ResolvedDependencies;

                });


                List<Task> tasksToWaitOn = new List<Task>();

                foreach (var th in sortedTaskList)
                {
                    var thCapture = th;

                    tasksToWaitOn.Add(thCapture.ScheduledTask);

                    var task = new Task(() =>
                    {
                        if (thCapture.ResolvedDependencies != null)
                        {
                            var depWaitList = thCapture.ResolvedDependencies.Select(t => t.ScheduledTask).ToArray();

                            //wait for dependent tasks to complete

                            Task.WaitAll(depWaitList, _ctsSource.Token);
                        }

                        thCapture.ScheduledTask.Start(_ts == null ? TaskScheduler.Default : _ts);

                    });

                    task.Start(_ts == null ? TaskScheduler.Default : _ts);

                }
                //wait for all tasks to complete before we mark the most outer task as completed, i.e. wating on RunTasks() only returns when all child tasks are complete
                Task.WaitAll(tasksToWaitOn.ToArray(), _ctsSource.Token);

            });


            _lastRunTask.Start(_ts == null ? TaskScheduler.Default : _ts);

            return _lastRunTask;

        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void RequestCancellation()
        {
            _ctsSource.Cancel(true);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void RequestCancellationAfter(int ms)
        {
            _ctsSource.CancelAfter(ms);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Reset()
        {
            _scheduledTasks.Clear();

            if (!_ctsSource.IsCancellationRequested)
                _ctsSource.Cancel();

            _ctsSource = new CancellationTokenSource();

            _lastRunTask = null;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Dispose()
        {
            this.Reset();

        }
    }

}
