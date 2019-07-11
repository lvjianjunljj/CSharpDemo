#pragma once

#include <initializer_list>
#include <string>
#include <vector>
#include <map>

namespace ScopeCommon
{
    typedef unsigned int DWORD;
    typedef unsigned char BYTE;

    // Basic details of an error as read from the message dictionary
    class ScopeMessageTemplate
    {
    public:
        DWORD DiagnosticCode;
        DWORD Severity;
        std::string Component;
        DWORD Source;
        std::string Id;
        std::string Message;
        std::string Description;
        std::string Resolution;
        std::string HelpLink;
    };

    // ScopeErrorArgs are inserted replacing parameters in the messages
    // - this is a simple mechanism for providing paraneters of different types in an easy way
    // - each parameter is internally converted to a string
    // - we could use an alternative variant style design if more formatting flexibility is ever needed
    class ScopeErrorArg
    {
    private:
        std::string Value;

    public:
        ScopeErrorArg(const std::string &val) : Value(val)
        {
        }

        ScopeErrorArg(const char * val) : Value(val == nullptr ? "<nullptr>" : val)
        {
        }

        ScopeErrorArg(const int val) : Value(std::to_string(val))
        {
        }

        ScopeErrorArg(const unsigned val) : Value(std::to_string(val))
        {
        }

        ScopeErrorArg(const __int64 val) : Value(std::to_string(val))
        {
        }

        ScopeErrorArg(const unsigned __int64 val) : Value(std::to_string(val))
        {
        }

        ScopeErrorArg(const double val) : Value(std::to_string(val))
        {
        }

        std::string ToString() const
        {
            return Value;
        }
    };

    // The exception used when error handling fails
    class InternalError : public std::exception
    {
    private:
        std::string m_message;
    public:
        InternalError(const std::string& message);
        std::string GetMessage() const { return m_message; }
    };

    // A scope error is a formatted version of a template
    // This is the actual ScopeErrorMessage returned from the message Dictionary lookup
    class ScopeErrorMessage : public ScopeMessageTemplate
    {
    private:
        // [spuchin] TODO: Change to ScopeErrorMessage after enabling JSON error support in JM.
        std::string m_innerError;
    public:
        std::string Details;
        std::string InternalDiagnostics;

        // Default constructor
        ScopeErrorMessage();

        // Destructor to avoid C4265 compiler warning.
        virtual ~ScopeErrorMessage();
        
        // Get inner error
        std::string InnerError() const { return m_innerError; }

        // Add additional details to give more information to the user
        void AddDetailsForUser(const std::string &details);

        // Add additional diagnostics for Microsoft support (note users do not generally see this)
        void AddInternalDiagnostics(const std::string &diagnostics);

        // Set inner error
        void SetInnerError(const ScopeErrorMessage& innerError);

        // Set inner error as JSON string
        // [spuchin] TODO: Deprecate once m_innerError type is changed to ScopeErrorMessage.
        void SetInnerError(const std::string& jsonInnerError);

        // Serialize to JSON interchange format
        std::string Serialize() const;
    private:
        // Escape a string for JSON serialization
        static std::string EscapeString(const std::string &input);
    };

    // Message Dictionary class used to load messages corresponding to error codes
    class MessageDictionary
    {
        std::map<int, ScopeMessageTemplate> m_index;

    public:
        // Create a message dictionary from a named DLL resource (uses current DLL to load resource from)
        MessageDictionary(const std::string & resourceName);

        // Lookup specified error code, return message expanded with supplied parameters
        ScopeErrorMessage Lookup(int diagnosticCode, std::initializer_list<ScopeErrorArg> args);

        // Sometimes we need to get all error codes. E.g. adding to CsError table.
        std::vector<ScopeMessageTemplate> GetAllErrors();
    };

}
