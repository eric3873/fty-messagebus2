#include <catch2/catch.hpp>
#include <fty/messagebus2/Promise.h>
#include <fty/messagebus2/MessageBus.h>

#include <iostream>

namespace {
//----------------------------------------------------------------------
// Test case
//----------------------------------------------------------------------
using namespace fty::messagebus2;

TEST_CASE("Promise with void", "[Promise]")
{
    Promise<void> myPromise;
    REQUIRE(myPromise.isReady());
    REQUIRE(!myPromise.waitFor(100));
    REQUIRE(myPromise.setValue());
    // Promise already satisfied
    REQUIRE(!myPromise.setValue());
    REQUIRE(myPromise.isReady());
    REQUIRE(myPromise.waitFor(100));
    REQUIRE(myPromise.getValue());
    REQUIRE(!myPromise.isReady());
    REQUIRE(!myPromise.getValue());
    REQUIRE(!myPromise.waitFor(100));

    myPromise.reset();
    REQUIRE(myPromise.isReady());
    REQUIRE(!myPromise.waitFor(100));
    REQUIRE(myPromise.setValue());
    // Promise already satisfied
    REQUIRE(!myPromise.setValue());
    REQUIRE(myPromise.isReady());
    REQUIRE(myPromise.waitFor(100));
    REQUIRE(myPromise.getValue());
    REQUIRE(!myPromise.isReady());
    REQUIRE(!myPromise.getValue());
    REQUIRE(!myPromise.waitFor(100));
}

TEST_CASE("Promise with ComState", "[Promise]")
{
    ComState state;
    Promise<ComState> myPromise;
    REQUIRE(myPromise.isReady());
    REQUIRE(!myPromise.waitFor(100));
    REQUIRE(myPromise.setValue(state));
    // Promise already satisfied
    REQUIRE(!myPromise.setValue(state));
    REQUIRE(myPromise.isReady());
    REQUIRE(myPromise.waitFor(100));
    REQUIRE(*(myPromise.getValue()) == state);
    REQUIRE(!myPromise.isReady());
    REQUIRE(!myPromise.getValue());
    REQUIRE(!myPromise.waitFor(100));

    myPromise.reset();
    REQUIRE(myPromise.isReady());
    REQUIRE(!myPromise.waitFor(100));
    REQUIRE(myPromise.setValue(state));
    // Promise already satisfied
    REQUIRE(!myPromise.setValue(state));
    REQUIRE(myPromise.waitFor(100));
    REQUIRE(myPromise.isReady());
    REQUIRE(*(myPromise.getValue()) == state);
    REQUIRE(!myPromise.isReady());
    REQUIRE(!myPromise.getValue());
    REQUIRE(!myPromise.waitFor(100));
}

TEST_CASE("Promise with Message", "[Promise]")
{
    class MessageBusForTest final : public MessageBus
    {
    public:
        MessageBusForTest() = default;
        ~MessageBusForTest() = default;

        MessageBusForTest(MessageBusForTest&&) = delete;
        MessageBusForTest& operator = (MessageBusForTest&&) = delete;
        MessageBusForTest(const MessageBusForTest&) = delete;
        MessageBusForTest& operator = (const MessageBusForTest&) = delete;

        fty::Expected<void, ComState> connect() noexcept { return {}; };
        fty::Expected<void, DeliveryState> send(const Message& msg) noexcept override {
            m_messageListener(msg);
            return {};
        };
        fty::Expected<void, DeliveryState> receive(const Address&, MessageListener&& messageListener, const std::string&) noexcept override {
            m_messageListener = messageListener;
            return {};
        };
        fty::Expected<void, DeliveryState> unreceive(const Address&, const std::string&) noexcept override {
            m_messageListener = nullptr;
            return {};
        };
        fty::Expected<Message, DeliveryState> request(const Message&, int) noexcept override { Message message; return message; };
        const ClientName& clientName() const noexcept override { return m_clientName; };
        const Identity& identity() const noexcept override { return m_identity; };
        const MessageListener getMessageListener() {
            return m_messageListener;
        };
    private:
        ClientName m_clientName;
        Identity   m_identity;
        MessageListener m_messageListener;
    };

    MessageBusForTest myMessageBus;
    Message msg;
    msg.correlationId("myCorrelationId");
    {
        Promise<Message> myPromise(myMessageBus, "myQueue", msg.correlationId());
        REQUIRE(myPromise.isReady());
        REQUIRE(!myPromise.waitFor(100));
        myPromise.setValue(msg);
        REQUIRE(myPromise.isReady());
        REQUIRE(myPromise.waitFor(100));
        REQUIRE((*(myPromise.getValue())).toString() == msg.toString());
        REQUIRE(!myPromise.isReady());
        REQUIRE(!myPromise.getValue());
        REQUIRE(!myPromise.waitFor(100));

        myPromise.reset();
        REQUIRE(myPromise.isReady());
        REQUIRE(!myPromise.waitFor(100));
        myPromise.setValue(msg);
        REQUIRE(myPromise.isReady());
        REQUIRE(myPromise.waitFor(100));
        REQUIRE((*(myPromise.getValue())).toString() == msg.toString());
        REQUIRE(!myPromise.isReady());
        REQUIRE(!myPromise.getValue());
        REQUIRE(!myPromise.waitFor(100));

        myPromise.reset();
        REQUIRE(myPromise.isReady());
        // bind the function to the reply queue
        REQUIRE(myMessageBus.receive(
            msg.replyTo(),
            std::move(std::bind(&Promise<Message>::setValue, &myPromise, std::placeholders::_1)),
            msg.correlationId()
        ));
        // send the message
        REQUIRE(myMessageBus.send(msg));
        REQUIRE(myPromise.waitFor(100));
        REQUIRE((*(myPromise.getValue())).toString() == msg.toString());
    }
    // check that unreceive has been call
    REQUIRE(!myMessageBus.getMessageListener());
}

} // namespace
