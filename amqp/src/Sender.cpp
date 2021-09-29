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

#include "Sender.h"

#include <proton/tracker.hpp>
#include <proton/work_queue.hpp>

#include <fty_log.h>

namespace fty::messagebus::amqp
{
  void Sender::on_container_start(proton::container& container)
  {
    logDebug("Sender on_container_start");
    try
    {
      container.connect(m_url);
    }
    catch (std::exception& e)
    {
      log_error("Exception {}", e.what());
    }
  }

  void Sender::on_connection_open(proton::connection& connection)
  {
    logDebug("Sender on_connection_open for target address: {}", m_address);
    connection.open_sender(m_address);
  }

  void Sender::on_sender_open(proton::sender& sender)
  {
    logDebug("on_sender_open");
    std::unique_lock<std::mutex> l(m_lock);
    m_sender = sender;
    m_cvSenderReady.notify_all();
  }

  void Sender::sendMsg(const proton::message& msg)
  {
    logDebug("Init sender waiting...");
    std::unique_lock<std::mutex> l(m_lock);
    m_cvSenderReady.wait(l);
    logDebug("sender ready on {}", msg.to().c_str());

    m_sender.work_queue().add([=]() {
      logDebug("Msg to sent {}", proton::to_string(msg));
      auto tracker = m_sender.send(msg);
      logDebug("Msg sent {}", proton::to_string(tracker.state()));
      m_sender.connection().close();
      logDebug("Sender closed");
    });
  }

  void Sender::close()
  {
    std::lock_guard<std::mutex> l(m_lock);
    logDebug("Closing sender for {}", m_address);
    if (m_sender)
    {
      m_sender.connection().close();
    }
    logDebug("Closed");
  }

} // namespace fty::messagebus::amqp
