#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <vector>
#include <string>
#include <queue>
#include <chrono>
#include <thread>


// very simple thread pool


class ThreadPool {

private:
	std::vector<std::thread> threads;
	std::queue<std::function<void()> > tasks;
	std::mutex mutque;
	std::condition_variable conv;
	bool end = false;




public:
	// Constructor: spawns num_th worker threads (defaults to the number of
	// hardware threads). Each worker runs the same loop for the pool's lifetime:
	//   1. Lock the queue mutex and sleep on the condition variable until
	//      either a task is available or shutdown (end) has been requested.
	//   2. If shutting down and the queue is drained, exit the thread.
	//   3. Otherwise take the front task off the queue, release the lock,
	//      and run the task. The lock is held only while touching the queue,
	//      never while executing the task, so workers run tasks in parallel.
	ThreadPool(size_t num_th
		= std::thread::hardware_concurrency()) {

		for (size_t i = 0; i < num_th; i++) {
			threads.emplace_back([this] {
				while (true) {
					std::function<void()> task;
					{
						std::unique_lock<std::mutex> lock(mutque);
						conv.wait(lock, [this] {
							return !tasks.empty() || end == true;
						});
						if (end && tasks.empty()) {
							return;
							}
						task = std::move(tasks.front());
						tasks.pop();
					}
					task();
			 }

     	 });

		}
	}
	// Destructor: shuts the pool down gracefully.
	//   1. Set the end flag under the mutex so no worker misses it.
	//   2. Wake every sleeping worker with notify_all.
	//   3. Join each thread. Workers keep pulling tasks until the queue is
	//      empty, so anything enqueued before destruction still runs; the
	//      destructor blocks until all of that work has finished.
	~ThreadPool() {
		{
			std::unique_lock<std::mutex> lock(mutque);
			end = true;
		}
		conv.notify_all();
		for (auto& thread : threads) {
			thread.join();
		}
	}

	// enq: submits a task to the pool.
	//   1. Push the callable onto the queue while holding the mutex.
	//   2. Release the lock, then wake one sleeping worker to pick it up.
	//      Notifying after unlocking avoids waking a thread that would
	//      immediately block trying to acquire the still-held mutex.
	void enq(std::function<void()> task) {
		{
			std::unique_lock<std::mutex> lock(mutque);
			tasks.emplace(std::move(task));
		}
		conv.notify_one();
	}
};

// is_prime: trial division up to sqrt(n). Deliberately naive so each task
// has a real CPU-bound workload to chew on.
static bool is_prime(long long n) {
	if (n < 2) return false;
	for (long long d = 2; d * d <= n; d++) {
		if (n % d == 0) return false;
	}
	return true;
}

// count_primes_in_range: counts primes in [begin, end). This is the unit of
// work each pool task performs on its own chunk of the full range.
static long long count_primes_in_range(long long begin, long long end) {
	long long count = 0;
	for (long long n = begin; n < end; n++) {
		if (is_prime(n)) count++;
	}
	return count;
}

// main: demo driver for the pool — counts the primes below `limit` in parallel.
//   The full range is split into num_tasks chunks and each chunk is enqueued
//   as one task. Every task writes its partial count into its own slot of
//   `results`, so no locking is needed between tasks. The pool lives inside
//   an inner scope: when that scope closes, the destructor joins the workers,
//   which guarantees every task has finished (and its write to `results` is
//   visible) before the partial counts are summed.
int main() {
	constexpr long long limit = 2'000'000;
	constexpr int num_tasks = 16;
	constexpr long long chunk = limit / num_tasks;

	std::vector<long long> results(num_tasks, 0);

	auto start = std::chrono::steady_clock::now();
	{
		ThreadPool pool(4);

		for (int i = 0; i < num_tasks; i++) {
			long long begin = i * chunk;
			long long end = (i == num_tasks - 1) ? limit : begin + chunk;

			pool.enq([i, begin, end, &results] {
				std::cout << "Task " << i << " scanning [" << begin << ", " << end
					<< ") on thread " << std::this_thread::get_id() << '\n';
				results[i] = count_primes_in_range(begin, end);
				});
		}
	} // pool destructor runs here: waits for all tasks, then joins the workers

	long long total = 0;
	for (long long partial : results) {
		total += partial;
	}
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::steady_clock::now() - start);

	std::cout << "Found " << total << " primes below " << limit
		<< " in " << elapsed.count() << " ms\n";
	return 0;
}




 




