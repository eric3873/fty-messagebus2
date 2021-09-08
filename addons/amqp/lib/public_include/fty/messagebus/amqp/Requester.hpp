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
    proton::duration m_timeout;
    proton::message request;

    proton::connection m_connection;

    proton::sender m_sender;
    proton::receiver m_receiver;
    proton::work_queue *p_work_queue;
    bool ready, canceled;

  public:
    Requester(const std::string& url, const proton::message& message, int timeout)
      : m_url(url)
      , request(message)
      , m_timeout(int(timeout * proton::duration::SECOND.milliseconds()))
    {
    }

    void on_container_start(proton::container& con) override
    {
      log_debug("on_container_start");
      m_connection = con.connect(m_url);//, proton::connection_options().idle_timeout(m_timeout));

      m_sender = m_connection.open_sender(request.to());
      // Create a receiver requesting a dynamically created queue
      // for the message source.
      receiver_options opts = receiver_options().source(source_options().dynamic(true));
      m_receiver = m_sender.connection().open_receiver("", opts);
    }

    void send_request()
    {
      log_debug("send_request");
      // TODO see where to set this.
      request.reply_to(m_receiver.source().address());
      m_sender.send(request);
    }

    void on_sender_open(proton::sender& s) override
    {
      log_debug("Open sender for target address: %s", s.target().address().c_str());
    }

    void on_receiver_open(proton::receiver& receiver) override
    {
      log_debug("Open receiver for target address: %s", receiver.source().address().c_str());
      p_work_queue = &receiver.work_queue();
      p_work_queue->schedule(m_timeout, make_work(&Requester::cancel, this, receiver));
      send_request();
    }

    void cancel(proton::receiver receiver)
    {
        log_debug("Cancel");
        canceled = true;

        m_receiver.connection().close();
        m_sender.connection().close();
        m_connection.close();
        //m_connection.container().stop();
        log_debug("Canceled");

    }

    void on_message(proton::delivery& d, proton::message& response) override
    {
      std::cout << " response: " << response.body() << std::endl;
      d.connection().close();
      //delete *p_work_queue;
      cancel(d.receiver());
      //d.connection().close();
    }
  };

} // namespace fty::messagebus::amqp
