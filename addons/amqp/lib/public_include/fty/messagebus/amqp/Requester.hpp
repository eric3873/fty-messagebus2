/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/delivery.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver_options.hpp>
#include <proton/source_options.hpp>
#include <proton/tracker.hpp>

#include <iostream>
#include <vector>

namespace fty::messagebus::amqp
{
  using proton::receiver_options;
  using proton::source_options;

  class Requester : public proton::messaging_handler
  {
  private:
    std::string m_url;
    proton::message request;
    proton::sender sender;
    proton::receiver receiver;

  public:
    Requester(const std::string& url, const proton::message& message)
      : m_url(url)
      , request(message)
    {
    }

    void on_container_start(proton::container& con) override
    {
      log_debug("on_container_start");
      proton::connection connection = con.connect(m_url);
      sender = connection.open_sender(request.to());
      // Create a receiver requesting a dynamically created queue
      // for the message source.
      receiver_options opts = receiver_options().source(source_options().dynamic(true));
      receiver = sender.connection().open_receiver("", opts);
    }

    void send_request()
    {
      log_debug("send_request");
      request.reply_to(receiver.source().address());
      sender.send(request);
      sender.connection().close();
    }

    void on_sender_open(proton::sender& s) override
    {
      log_debug("Open sender for target address: %s", s.target().address().c_str());
      send_request();
    }

    void on_receiver_open(proton::receiver& r) override
    {
      log_debug("Open receiver for target address: %s", r.source().address().c_str());
      send_request();
    }

    void on_message(proton::delivery& d, proton::message& response) override
    {
      std::cout << " response: " << response.body() << std::endl;
      d.connection().close();
    }
  };

} // namespace fty::messagebus::amqp
