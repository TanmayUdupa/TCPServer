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
#include <sys/socket.h>
#include <netinet/in.h>
#include <iostream>
#include <unistd.h>
#include <map>
#include <sstream>
using namespace std;

#define BUFFER_SIZE 1024


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
  ssize_t bytes_received;
  char buffer[BUFFER_SIZE];
  map<string, string> KV_DATASTORE;

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
          KV_DATASTORE[key] = value.substr(1);
          response += "FIN\n";
        }
        else if (line == "READ")
        {
          getline(stream, key);
          if (KV_DATASTORE.find(key) != KV_DATASTORE.end())
          {
            response += KV_DATASTORE[key] + "\n";
          }
          else
          {
            response += "NULL\n";
          }
        }
        else if(line == "COUNT")
        {
          response += to_string(KV_DATASTORE.size()) + "\n";
        }
        else if (line == "DELETE")
        {
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
        else if (line == "END")
        {
          send(client_socket, response.c_str(), response.length(), 0);
          close(client_socket);
        }
      }
    }
    close(server_socket);

    return 0;
}