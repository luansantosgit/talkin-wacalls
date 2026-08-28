# Stage 1: Builder
FROM golang:1.26-alpine AS builder

# Instala dependências necessárias
RUN apk add --no-cache git nodejs npm build-base python3

WORKDIR /app

# Copia arquivos de dependência primeiro (cache otimizado)
COPY go.mod go.sum ./
RUN go mod download

# Copia o restante do código
COPY . .

# Compila o frontend
RUN cd client && npm install && npm run build

# Compila o backend Go
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o server ./cmd/server

# Stage 2: Imagem final leve
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# Copia o binário compilado e os arquivos estáticos
COPY --from=builder /app/server .
COPY --from=builder /app/client/dist ./client/dist

EXPOSE 8080

CMD ["./server", "-addr", ":8080", "-static", "client/dist"]