/* 
 * tcpserver.c - A multithreaded TCP echo server 
 * usage: tcpserver <port>
 * 
 * Testing : 
 * nc localhost <port> < input.txt
 */

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <iostream>
#include <unistd.h>
#include <map>
#include <sstream>
#include <mutex>
#include <queue>
#include <condition_variable>
using namespace std;

#define BUFFER_SIZE 1024

map<string, string> KV_DATASTORE;
queue<int> q;
mutex q_mutex;
mutex KV_DATASTORE_mutex;
condition_variable cv;

void *func(void* arg)
{
  while(true)
  {
  ssize_t bytes_received;
  char buffer[BUFFER_SIZE];
  int client_socket;
  {
    unique_lock<mutex> lock(q_mutex);
    cv.wait(lock, []{return !q.empty();});
    client_socket = q.front();
    q.pop();
  }
       string message = "";
      while ((bytes_received = recv(client_socket, buffer, BUFFER_SIZE, 0)) > 0) {
            message.append(buffer, bytes_received);
        }
      cout << message << endl;
      istringstream stream(message);
      string line;
      string response = "";
      while(getline(stream, line))
      {
        string key, value;
        if (line == "WRITE")
        {
          getline(stream, key);
          getline(stream, value);
          {
            lock_guard<mutex> lock(KV_DATASTORE_mutex);
            KV_DATASTORE[key] = value.substr(1);
          }
          response += "FIN\n";
        }
        else if (line == "READ")
        {
          getline(stream, key);
          {
          lock_guard<mutex> lock(KV_DATASTORE_mutex);
          if (KV_DATASTORE.find(key) != KV_DATASTORE.end())
          {
            response += KV_DATASTORE[key] + "\n";
          }
          else
          {
            response += "NULL\n";
          }
          }
        }
        else if(line == "COUNT")
        {
          {
            lock_guard<mutex> lock(KV_DATASTORE_mutex);
            response += to_string(KV_DATASTORE.size()) + "\n";
          }
        }
        else if (line == "DELETE")
        {
          getline(stream, key);
          {
          lock_guard<mutex> lock(KV_DATASTORE_mutex);
          if (KV_DATASTORE.find(key) != KV_DATASTORE.end())
          {
            KV_DATASTORE.erase(key);            
            response += "FIN\n";
          }
          else
          {
            response += "NULL\n";
          }
          }
        }
        else if (line == "END")
        {
          send(client_socket, response.c_str(), response.length(), 0);
          close(client_socket);
        }
      }
  }
  return NULL;
}


int main(int argc, char ** argv) {
  int portno; /* port to listen on */
  
  /* 
   * check command line arguments 
   */
  if (argc != 2) {
    fprintf(stderr, "usage: %s <port>\n", argv[0]);
    exit(1);
  }

  // DONE: Server port number taken as command line argument
  portno = atoi(argv[1]);

   struct sockaddr_in server_address, client_address;
  int client_socket;
  socklen_t client_len = sizeof(client_address);

  pthread_t threads[10];
  for (int i = 0; i < 10; i++)
  {
    pthread_create(&threads[i], NULL, func, NULL);
  }

  int server_socket = socket(AF_INET, SOCK_STREAM, 0);

    memset((char *)&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(portno);

    bind(server_socket, (struct sockaddr *)&server_address, sizeof(server_address));
    listen(server_socket, 5);

    while (true)
    {
      client_socket = accept(server_socket, (struct sockaddr *)&client_address, &client_len);
      {
        lock_guard<mutex> lock(q_mutex);
        q.push(client_socket);
        cv.notify_one();
      }
    }
    close(server_socket);

    return 0;
}