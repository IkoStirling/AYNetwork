#include <AYTest.h>

#include <cstdio>

int main(int argc, char** argv)
{
    // Network integration tests may wait on real sockets. Keep progress
    // visible and allow a single suite to be selected while diagnosing them.
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    return ayt::test::runTests("AYNetwork", argc, argv);
}
