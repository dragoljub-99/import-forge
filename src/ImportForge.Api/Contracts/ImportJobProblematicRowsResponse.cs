namespace ImportForge.Api.Contracts;

public sealed record ImportJobProblematicRowsResponse(
    long JobId,
    int ProblematicRowsCount,
    IReadOnlyList<ProblematicRowDto> Rows);
