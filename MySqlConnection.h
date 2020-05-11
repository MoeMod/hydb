#pragma once

#include <boost/mysql/mysql.hpp>

class MySqlConnection : public std::enable_shared_from_this<MySqlConnection>
{
public:
    MySqlConnection(DatabaseConfig config, std::shared_ptr<boost::asio::io_context> io_context) :
        dbc(std::move(config)),
        conn_params(dbc.user, dbc.pass, dbc.schema, boost::mysql::collation::utf8_general_ci, boost::mysql::ssl_options(boost::mysql::ssl_mode::disable)),
        ioc(std::move(io_context)),
        resolver(*ioc),
        connection(*ioc)
    {

    }
    ~MySqlConnection()
    {
        boost::system::error_code ec;
        boost::mysql::error_info ei;
        connection.quit(ec, ei);
        connection.close(ec, ei);
    }

public:
    const DatabaseConfig dbc;
    const std::shared_ptr<boost::asio::io_context> ioc;
    boost::mysql::connection_params conn_params;  // MySQL credentials and other connection config
    boost::asio::ip::tcp::resolver resolver;
    boost::mysql::tcp_connection connection;
    boost::asio::ip::tcp::endpoint endpoint;

    void start()
    {
        resolver.async_resolve(
                dbc.host,
                dbc.port,
                std::bind(
                        &MySqlConnection::on_resolve,
                        this->shared_from_this(),
                        std::placeholders::_1,
                        std::placeholders::_2));
    }

    void on_resolve(
            boost::system::error_code ec,
            boost::asio::ip::tcp::resolver::results_type results)
    {
        if (ec)
            return fail(ec, "resolve");
        boost::asio::async_connect(connection.next_layer(),
                results.begin(), results.end(),
                std::bind(&MySqlConnection::on_connect, this->shared_from_this(), std::placeholders::_1)
            );
    }

    void on_connect(boost::system::error_code ec) {
        if (ec)
            return fail(ec, "connect");

        connection.async_handshake(conn_params,
               std::bind(&MySqlConnection::on_handshake, this->shared_from_this(), std::placeholders::_1)
        );
    }

    void on_handshake(boost::system::error_code ec) {
        if (ec)
        {
            connection.close();
            return fail(ec, "handshake");
        }

        pro_connected.set_value();

        start_ping(ec);
    }

    void start_ping(const boost::system::error_code& ec)
    {
        if (ec)
            return fail(ec, "start_ping");
        using namespace std::chrono_literals;
        accessor.reset();
        std::shared_ptr<boost::asio::system_timer> st = std::make_shared<boost::asio::system_timer>(*ioc);
        st->expires_after(1min);
        st->async_wait(std::bind(&MySqlConnection::on_ping, this->shared_from_this(), std::placeholders::_1));
    }

    void on_ping(const boost::system::error_code& ec)
    {
        if (ec)
            return fail(ec, "on_ping");
        pro_connected = {};
        fut_connected = pro_connected.get_future();
        if (accessor.lock() == nullptr)
        {
            // unique connection here
            accessor = shared_from_this();
            connection.async_query("SELECT 1=1;", std::bind(&MySqlConnection::start_ping, this->shared_from_this(), std::placeholders::_1));
        }
        else
        {
            start_ping(ec);
        }
    }

    void fail(boost::system::error_code ec, const std::string &what) {
        try {
            pro_connected.set_exception(std::make_exception_ptr(std::system_error(ec, what + ": " + ec.message())));
        } catch(std::future_error &){
            // ignored
        }
    }

    void wait_for_ready() const
    {
        fut_connected.get();
    }

public:
    std::promise<void> pro_connected;
    std::shared_future<void> fut_connected = pro_connected.get_future();
    std::weak_ptr<void> accessor;
};