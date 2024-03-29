//
// Created by 梅峰源 on 2020-04-26.
//

#ifndef CQMIAO_GLOBALCONTEXT_H
#define CQMIAO_GLOBALCONTEXT_H

#include <memory>

namespace boost::asio {
    class io_context;
}

std::shared_ptr<boost::asio::io_context> GlobalContextSingleton();


#endif //CQMIAO_GLOBALCONTEXT_H
