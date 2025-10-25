FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /source
COPY Directory.Build.props Directory.Packages.props ./
COPY src/PartnerSync.Contracts/PartnerSync.Contracts.csproj src/PartnerSync.Contracts/
COPY src/PartnerSync/PartnerSync.csproj src/PartnerSync/
COPY src/DemoSystems/DemoSystems.csproj src/DemoSystems/
RUN dotnet restore src/PartnerSync/PartnerSync.csproj && dotnet restore src/DemoSystems/DemoSystems.csproj
COPY src/ src/
RUN dotnet publish src/PartnerSync/PartnerSync.csproj -c Release --no-restore -o /out/sync
RUN dotnet publish src/DemoSystems/DemoSystems.csproj -c Release --no-restore -o /out/demo

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS sync
WORKDIR /app
COPY --from=build /out/sync .
USER $APP_UID
EXPOSE 8080
ENTRYPOINT ["dotnet", "PartnerSync.dll"]

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS demo
WORKDIR /app
COPY --from=build /out/demo .
USER $APP_UID
EXPOSE 8081 8082
ENTRYPOINT ["dotnet", "DemoSystems.dll"]
