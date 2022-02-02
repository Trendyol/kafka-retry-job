FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build

WORKDIR /src
COPY /src .

RUN dotnet publish "kafka.retry.job.csproj" -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:5.0

WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "kafka.retry.job.dll"]
