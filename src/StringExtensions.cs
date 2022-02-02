using System;

namespace KafkaRetry.Job
{
    public static class StringExtensions
    {
        public static string ReplaceAtEnd(this string input, string oldValue, string newValue)
        {
            if (!input.EndsWith(oldValue))
            {
                throw new InvalidOperationException($"{input} does not end with {oldValue}.");
            }

            var lastIndex = input.LastIndexOf(oldValue, StringComparison.Ordinal);
            var inputWithoutOldValueAtEnd = input.Substring(0, lastIndex);
            return inputWithoutOldValueAtEnd + newValue;
        }
    }
}