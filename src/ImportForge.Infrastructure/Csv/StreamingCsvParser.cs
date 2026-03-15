using System.Runtime.CompilerServices;
using System.Text;

namespace ImportForge.Infrastructure.Csv;

public sealed class StreamingCsvParser
{
    private enum FieldState
    {
        StartOfField = 0,
        InUnquotedField = 1,
        InQuotedField = 2,
        AfterQuotedField = 3
    }

    public async IAsyncEnumerable<CsvRowParseResult> ParseAsync(
        Stream stream,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var reader = new StreamReader(
            stream,
            Encoding.UTF8,
            detectEncodingFromByteOrderMarks: true,
            bufferSize: 4096,
            leaveOpen: true);

        var buffer = new char[4096];
        var currentField = new StringBuilder();
        var columns = new List<string>(capacity: 4);

        var fieldState = FieldState.StartOfField;
        var rowNumber = 0;
        var rowIsMalformed = false;
        var rowHasAnyCharacter = false;
        var pendingCarriageReturn = false;

        while (true)
        {
            var charsRead = await reader.ReadAsync(buffer.AsMemory(0, buffer.Length), ct);
            if (charsRead == 0)
            {
                break;
            }

            for (var i = 0; i < charsRead; i++)
            {
                ct.ThrowIfCancellationRequested();
                var ch = buffer[i];

                if (pendingCarriageReturn)
                {
                    pendingCarriageReturn = false;
                    yield return FinalizeRow();

                    if (ch == '\n')
                    {
                        continue;
                    }
                }

                if (ch == '\r')
                {
                    pendingCarriageReturn = true;
                    continue;
                }

                if (ch == '\n')
                {
                    yield return FinalizeRow();
                    continue;
                }

                ConsumeChar(ch);
            }
        }

        if (pendingCarriageReturn)
        {
            yield return FinalizeRow();
        }

        if (HasPendingRowData())
        {
            yield return FinalizeRow();
        }

        void ConsumeChar(char ch)
        {
            rowHasAnyCharacter = true;

            if (rowIsMalformed)
            {
                return;
            }

            switch (fieldState)
            {
                case FieldState.StartOfField:
                    if (ch == ',')
                    {
                        columns.Add(string.Empty);
                        return;
                    }

                    if (ch == '"')
                    {
                        fieldState = FieldState.InQuotedField;
                        return;
                    }

                    currentField.Append(ch);
                    fieldState = FieldState.InUnquotedField;
                    return;

                case FieldState.InUnquotedField:
                    if (ch == ',')
                    {
                        columns.Add(currentField.ToString());
                        currentField.Clear();
                        fieldState = FieldState.StartOfField;
                        return;
                    }

                    if (ch == '"')
                    {
                        MarkMalformed();
                        return;
                    }

                    currentField.Append(ch);
                    return;

                case FieldState.InQuotedField:
                    if (ch == '"')
                    {
                        fieldState = FieldState.AfterQuotedField;
                        return;
                    }

                    currentField.Append(ch);
                    return;

                case FieldState.AfterQuotedField:
                    if (ch == '"')
                    {
                        currentField.Append('"');
                        fieldState = FieldState.InQuotedField;
                        return;
                    }

                    if (ch == ',')
                    {
                        columns.Add(currentField.ToString());
                        currentField.Clear();
                        fieldState = FieldState.StartOfField;
                        return;
                    }

                    MarkMalformed();
                    return;

                default:
                    throw new InvalidOperationException($"Unknown parser state '{fieldState}'.");
            }
        }

        void MarkMalformed()
        {
            rowIsMalformed = true;
            currentField.Clear();
            columns.Clear();
            fieldState = FieldState.StartOfField;
        }

        bool HasPendingRowData()
            => rowHasAnyCharacter || rowIsMalformed || columns.Count > 0 || currentField.Length > 0 || fieldState != FieldState.StartOfField;

        CsvRowParseResult FinalizeRow()
        {
            rowNumber++;

            CsvRowParseResult result;
            if (rowIsMalformed || fieldState == FieldState.InQuotedField)
            {
                result = new CsvRowParseResult(rowNumber, CsvRowParseKind.Malformed, Array.Empty<string>());
            }
            else
            {
                switch (fieldState)
                {
                    case FieldState.StartOfField:
                        columns.Add(string.Empty);
                        break;
                    case FieldState.InUnquotedField:
                    case FieldState.AfterQuotedField:
                        columns.Add(currentField.ToString());
                        break;
                }

                result = new CsvRowParseResult(rowNumber, CsvRowParseKind.Parsed, columns.ToArray());
            }

            currentField.Clear();
            columns.Clear();
            fieldState = FieldState.StartOfField;
            rowIsMalformed = false;
            rowHasAnyCharacter = false;

            return result;
        }
    }
}
