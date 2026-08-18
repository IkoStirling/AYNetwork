#include <AYTest.h>

#include <cstdio>

int main(int argc, char** argv)
{
    // Network integration tests may wait on real sockets. Keep progress
    // visible and allow a single suite to be selected while diagnosing them.
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    if (argc > 1) {
        return ayt::test::runSuite(argv[1]);
    }
    return ayt::test::runAllTests("AYNetwork_Test");
}
