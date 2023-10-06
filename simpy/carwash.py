import simpy

# Define a Task class
class Task:
    def __init__(self, name, processing_time):
        self.name = name
        self.processing_time = processing_time

# Define a function for the worker process
def worker(env, name, task_queue):
    while True:
        try:
            # Get the next task from the queue
            task = yield task_queue.get()
            tasks_in_progress[name] = task
            
            # Process the task
            print(f"{name} starts task {task.name} at {env.now}")
            for tick in range(task.processing_time):
                if (name not in tasks_in_progress): # our task has been stolen
                    break
                # Print worker status every tick
                print(f'{name} - Progress: {tick + 1}/{task.processing_time}, Remaining Tasks: {len(task_queue.items)}')
                if (name == "Worker 1"):
                    yield env.timeout(10)  # worker 1 sucks
                else:
                    yield env.timeout(1)  # Simulate one tick of processing time
            print(f'{name} completes or sheds task {task.name} at {env.now}')
            
        except simpy.Interrupt:
            # If interrupted, stop processing tasks
            print(f'{name} is interrupted at {env.now}')
            break

# Define a global scheduler process
def global_scheduler(env, workers, task_queues):
    while True:
        # Check worker progress
        for worker, worker_name, task_queue in zip(workers, worker_names, task_queues):
            if not task_queue.items or worker_name not in tasks_in_progress:
                continue  # Skip workers with no tasks

            current_task = tasks_in_progress[worker_name] # TODO this can crash
            if current_task.processing_time > 1:
                # If the task is not completed in one tick, check other workers' progress
                for other_worker, other_worker_name, other_task_queue in zip(workers, worker_names, task_queues):
                    if other_worker != worker and len(other_task_queue.items) < len(task_queue.items):
                        # Reassign the task to the other worker and interrupt the current worker
                        print(f'Global Scheduler reassigns task {current_task.name} from {worker_name} to {other_worker_name} at {env.now}')
                        other_task_queue.put(current_task)
                        del tasks_in_progress[worker_name]
                        break
                else:
                    # Continue processing the current task
                    continue
            else:
                # Continue processing the current task
                continue

        # Yield until the next check
        yield env.timeout(1)

# Initialize the simulation environment
env = simpy.Environment()

# Parameters
num_tasks = 4
num_workers = 2

# Create a task queue for each worker
task_queues = [simpy.Store(env) for _ in range(num_workers)]
tasks_in_progress = {}

# Create workers
worker_names = [f'Worker {i}' for i in range(num_workers)]
workers = [env.process(worker(env, name, task_queues[i])) for i, name in enumerate(worker_names)]

# Create the global scheduler
env.process(global_scheduler(env, workers, task_queues))

# Generate and enqueue tasks with varying processing times
tasks = [Task(f'Task {i}', i + 1) for i in range(num_tasks)]
for task in tasks:
    worker_index = tasks.index(task) % num_workers  # Distribute tasks evenly among workers
    task_queues[worker_index].put(task)

# Run the simulation
env.run(until=sum(task.processing_time for task in tasks))

# Interrupt workers to stop processing
for worker in workers:
    worker.interrupt()

print("Simulation completed.")

