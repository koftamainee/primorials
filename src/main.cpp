#include <errno.h>
#include <fcntl.h>
#include <mqueue.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cmath>
#include <iostream>
#include <string>

#include "bigint.h"
#include "colors.h"

#define SHM_NAME "/primes_shm"
#define SEM_NAME "/primes_sem"
#define MQ_NAME "/primorial_mq"

#define MSG_LEN 4096

#define INVALID_ARGUMENTS (-1)
#define FAILED_RECEIVE_MESSAGE (-2)
#define FAILED_SEND_MESSAGE (-3)
#define BIGINT_ERROR (-4)

struct SharedData {
  int prime;
  int has_new;
  int turn;
};

int primes_process(int upto, SharedData* data, sem_t* sem);
int proc_process(int upto, int start_i, int step, SharedData* data, sem_t* sem,
                 mqd_t mq);
void cleanup_resources(SharedData* data, int shm_fd, sem_t* sem, mqd_t mq);
int is_prime(int n);

static int g_shm_fd = -1;
static SharedData* g_data = (SharedData*)MAP_FAILED;
static sem_t* g_sem = SEM_FAILED;
static mqd_t g_mq = (mqd_t)-1;

void sigint_handler(int) {
  if (g_mq != (mqd_t)-1) {
    mq_close(g_mq);
    mq_unlink(MQ_NAME);
  }
  if (g_sem != SEM_FAILED) {
    sem_close(g_sem);
    sem_unlink(SEM_NAME);
  }
  if (g_data != MAP_FAILED) {
    munmap((void*)g_data, sizeof(SharedData));
  }
  if (g_shm_fd != -1) {
    close(g_shm_fd);
    shm_unlink(SHM_NAME);
  }
  _exit(1);
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <upto>\n", argv[0]);
    return INVALID_ARGUMENTS;
  }

  int upto = atoi(argv[1]);
  if (upto < 1) {
    fprintf(stderr, "Invalid upto number\n");
    return INVALID_ARGUMENTS;
  }

  struct sigaction sa{};
  sa.sa_handler = sigint_handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  sigaction(SIGINT, &sa, NULL);

  g_shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
  if (g_shm_fd == -1) {
    perror("shm_open");
    return EXIT_FAILURE;
  }

  if (ftruncate(g_shm_fd, sizeof(SharedData)) == -1) {
    perror("ftruncate");
    close(g_shm_fd);
    shm_unlink(SHM_NAME);
    return EXIT_FAILURE;
  }

  g_data = (SharedData*)mmap(NULL, sizeof(SharedData), PROT_READ | PROT_WRITE,
                             MAP_SHARED, g_shm_fd, 0);
  if (g_data == MAP_FAILED) {
    perror("mmap");
    close(g_shm_fd);
    shm_unlink(SHM_NAME);
    return EXIT_FAILURE;
  }

  memset(g_data, 0, sizeof(SharedData));
  g_data->prime = 0;
  g_data->has_new = 0;
  g_data->turn = 0;

  g_sem = sem_open(SEM_NAME, O_CREAT | O_EXCL, 0666, 1);
  if (g_sem == SEM_FAILED) {
    if (errno == EEXIST) {
      g_sem = sem_open(SEM_NAME, 0);
    }
    if (g_sem == SEM_FAILED) {
      perror("sem_open");
      munmap(g_data, sizeof(SharedData));
      close(g_shm_fd);
      shm_unlink(SHM_NAME);
      return EXIT_FAILURE;
    }
  }

  struct mq_attr attr{};
  attr.mq_flags = 0;
  attr.mq_maxmsg = 10;
  attr.mq_msgsize = MSG_LEN;
  attr.mq_curmsgs = 0;

  g_mq = mq_open(MQ_NAME, O_CREAT | O_RDWR, 0666, &attr);
  if (g_mq == (mqd_t)-1) {
    perror("mq_open");
    cleanup_resources(g_data, g_shm_fd, g_sem, g_mq);
    return EXIT_FAILURE;
  }

  const char* init = "1";
  if (mq_send(g_mq, init, strlen(init) + 1, 0) == -1) {
    perror("mq_send initial");
    cleanup_resources(g_data, g_shm_fd, g_sem, g_mq);
    return EXIT_FAILURE;
  }

  pid_t pid_primes = fork();
  if (pid_primes < 0) {
    perror("fork primes");
    cleanup_resources(g_data, g_shm_fd, g_sem, g_mq);
    return EXIT_FAILURE;
  }
  if (pid_primes == 0) {
    return primes_process(upto, g_data, g_sem);
  }

  pid_t pid_odd = fork();
  if (pid_odd < 0) {
    perror("fork proc odd");
    cleanup_resources(g_data, g_shm_fd, g_sem, g_mq);
    return EXIT_FAILURE;
  }
  if (pid_odd == 0) {
    return proc_process(upto, 1, 2, g_data, g_sem, g_mq);
  }

  pid_t pid_even = fork();
  if (pid_even < 0) {
    perror("fork proc even");
    cleanup_resources(g_data, g_shm_fd, g_sem, g_mq);
    return EXIT_FAILURE;
  }
  if (pid_even == 0) {
    return proc_process(upto, 2, 2, g_data, g_sem, g_mq);
  }

  int status;
  waitpid(pid_primes, &status, 0);
  waitpid(pid_odd, &status, 0);
  waitpid(pid_even, &status, 0);

  cleanup_resources(g_data, g_shm_fd, g_sem, g_mq);
  return 0;
}

void cleanup_resources(SharedData* data, int shm_fd, sem_t* sem, mqd_t mq) {
  if (mq != (mqd_t)-1) {
    mq_close(mq);
    mq_unlink(MQ_NAME);
  }
  if (sem != SEM_FAILED) {
    sem_close(sem);
    sem_unlink(SEM_NAME);
  }
  if (data != MAP_FAILED) {
    munmap(data, sizeof(SharedData));
  }
  if (shm_fd != -1) {
    close(shm_fd);
    shm_unlink(SHM_NAME);
  }
}

int primes_process(int upto, SharedData* data, sem_t* sem) {
  if (!data || sem == SEM_FAILED) {
    fprintf(stderr, C_GREEN "[PRIMES]" C_RESET " invalid args\n");
    return INVALID_ARGUMENTS;
  }
  std::cout << C_GREEN "[PRIMES]" C_RESET " started\n";

  int current_proc_turn = 1;
  int count = 0;
  for (int p = 2; count < upto; ++p) {
    if (!is_prime(p)) continue;

    count++;

    while (true) {
      if (sem_wait(sem) == -1) {
        perror("sem_wait");
        return -1;
      }
      int can_post = (data->turn == 0 && data->has_new == 0);
      if (!can_post) {
        sem_post(sem);
        usleep(1000);
        continue;
      }

      data->prime = p;
      data->has_new = 1;
      data->turn = current_proc_turn;
      current_proc_turn = (current_proc_turn == 1) ? 2 : 1;
      sem_post(sem);
      break;
    }

    std::cout << C_GREEN "[PRIMES]" C_RESET " published prime " << p
              << ": prime[" << count << "]\n";

    // usleep(p * 100 * 1000);

    while (true) {
      if (sem_wait(sem) == -1) {
        perror("sem_wait");
        return -1;
      }
      if (data->turn == 0 && data->has_new == 0) {
        sem_post(sem);
        break;
      }
      sem_post(sem);
      usleep(1000);
    }
  }

  std::cout << C_GREEN "[PRIMES]" C_RESET " finished\n";
  return 0;
}

int proc_process(int upto, int start_i, int step, SharedData* data, sem_t* sem,
                 mqd_t mq) {
  if (step != 2 || !data || sem == SEM_FAILED || mq == (mqd_t)-1) {
    fprintf(stderr, "[PROC] invalid args\n");
    return INVALID_ARGUMENTS;
  }
  if (start_i != 1 && start_i != 2) {
    fprintf(stderr, "[PROC] bad start_i\n");
    return INVALID_ARGUMENTS;
  }

  const char* name = (start_i == 1) ? C_CYAN "[PROC_2i+1]" C_RESET
                                    : C_MAGENTA "[PROC_2i]" C_RESET;
  std::cout << name << " started\n";

  char buf[MSG_LEN];
  int current_primorial = start_i;

  for (int i = start_i - 1; i < upto; i += 2) {
    while (true) {
      if (sem_wait(sem) == -1) {
        perror("sem_wait");
        return -1;
      }
      int my_turn_now = (data->turn == start_i && data->has_new == 1);
      if (!my_turn_now) {
        sem_post(sem);
        usleep(1000);
        continue;
      }

      ssize_t read_bytes = mq_receive(mq, buf, MSG_LEN, NULL);
      if (read_bytes == -1) {
        perror("mq_receive");
        sem_post(sem);
        return FAILED_RECEIVE_MESSAGE;
      }
      if (read_bytes >= MSG_LEN)
        buf[MSG_LEN - 1] = '\0';
      else
        buf[read_bytes] = '\0';

      try {
        bigint primorial(buf);
        primorial *= data->prime;

        std::string out = primorial.to_string();
        if (out.size() + 1 > (size_t)MSG_LEN) {
          fprintf(stderr, "%s primorial too large for mq (len=%zu)\n", name,
                  out.size());
          sem_post(sem);
          return FAILED_SEND_MESSAGE;
        }

        if (mq_send(mq, out.c_str(), out.size() + 1, 0) == -1) {
          perror("mq_send");
          sem_post(sem);
          return FAILED_SEND_MESSAGE;
        }

        data->has_new = 0;
        data->turn = 0;
        sem_post(sem);

        std::cout << name << " multiplied by " << data->prime << " -> " << out
                  << ": [" << current_primorial << "#]\n";
        current_primorial += 2;

      } catch (const std::exception& e) {
        sem_post(sem);
        fprintf(stderr, "%s bigint error: %s\n", name, e.what());
        return BIGINT_ERROR;
      }

      break;
    }
  }

  std::cout << name << " finished\n";
  return 0;
}

int is_prime(int n) {
  if (n < 2) return 0;
  int r = (int)sqrt((double)n);
  for (int i = 2; i <= r; ++i)
    if (n % i == 0) return 0;
  return 1;
}
