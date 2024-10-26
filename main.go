package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Struct para el salón
type Salon struct {
	ID       string `bson:"_id"`
	Nombre   string `bson:"nombre,omitempty"`
	Pais     string `bson:"pais,omitempty"`
	Terminal int    `bson:"terminal,omitempty"`
}

// Struct para el usuario
type User struct {
	ID                    string      `bson:"_id"`
	Username              string      `bson:"username,omitempty"`
	Password              string      `bson:"password,omitempty"`
	FirstName             string      `bson:"firstName,omitempty"`
	LastName              string      `bson:"lastName,omitempty"`
	AccountNonExpired     bool        `bson:"accountNonExpired,omitempty"`
	AccountNonLocked      bool        `bson:"accountNonLocked,omitempty"`
	CredentialsNonExpired bool        `bson:"credentialsNonExpired,omitempty"`
	Enabled               bool        `bson:"enabled,omitempty"`
	Authorities           []Authority `bson:"authorities,omitempty"`
}

// Struct para la autoridad del usuario
type Authority struct {
	Name string `bson:"name,omitempty"`
}

// Struct para el cliente
type Cliente struct {
	Ref string `bson:"$ref,omitempty"`
	ID  int64  `bson:"$id"`
}

// Struct para el banco
type Banco struct {
	Ref string `bson:"$ref,omitempty"`
	ID  string `bson:"$id"`
}

// Struct para el medio de pago
type MedioPago struct {
	Ref string `bson:"$ref,omitempty"`
	ID  string `bson:"$id"`
}

// Struct para el checkin
type Checkin struct {
	ID                             string    `bson:"_id"`
	CantidadAcompanantes           int       `bson:"cantidadAcompanantes,omitempty"`
	Firma                          string    `bson:"firma,omitempty"`
	FechaIngreso                   time.Time `bson:"fechaIngreso,omitempty"`
	Sincronizado                   bool      `bson:"sincronizado,omitempty"`
	CodigoAutorizacionTbk          string    `bson:"codigoAutorizacionTbk,omitempty"`
	TotalPagadoTbk                 int64     `bson:"totalPagadoTbk,omitempty"`
	TotalVoucherConvenio           int64     `bson:"totalVoucherConvenio,omitempty"`
	CantidadAcoPagadoTbk           int       `bson:"cantidadAcoPagadoTbk,omitempty"`
	CantidadAcoEnConvenio          int       `bson:"cantidadAcoEnConvenio,omitempty"`
	NumeroTarjeta                  string    `bson:"numeroTarjeta,omitempty"`
	Turno                          string    `bson:"turno,omitempty"`
	IngresoEspecial                bool      `bson:"ingresoEspecial,omitempty"`
	ServicioBChile                 bool      `bson:"servicioBChile,omitempty"`
	ReCheckin                      bool      `bson:"reCheckin,omitempty"`
	TitularAmount                  int64     `bson:"titularAmount,omitempty"`
	ItauAccessExchange             bool      `bson:"itauAccessExchange,omitempty"`
	ItauAccessExchangeOffline      bool      `bson:"itauAccessExchangeOffline,omitempty"`
	BancoChileCuposLibres          int       `bson:"bancoChileCuposLibres,omitempty"`
	BancoChileCuposPremium         int       `bson:"bancoChileCuposPremium,omitempty"`
	BancoChileCuposPromocionales   int       `bson:"bancoChileCuposPromocionales,omitempty"`
	BancoChileCuposIngresoReducido int       `bson:"bancoChileCuposIngresoReducido,omitempty"`
	MontoConvenio                  int64     `bson:"montoConvenio,omitempty"`
	MontoSinConvenio               int64     `bson:"montoSinConvenio,omitempty"`
	FalabellaRegister              bool      `bson:"falabellaRegister,omitempty"`
	FalabellaRegisterOffline       bool      `bson:"falabellaRegisterOffline,omitempty"`
	AccessMode                     string    `bson:"accessMode,omitempty"`
	User                           User      `bson:"user,omitempty"`
	Cliente                        Cliente   `bson:"cliente,omitempty"`
	Banco                          Banco     `bson:"banco,omitempty"`
	MedioPago                      MedioPago `bson:"medioPago,omitempty"`
}

// Struct para el cierre
type Cierre struct {
	ID          string    `bson:"_id"`
	Envio       time.Time `bson:"envio,omitempty"`
	Recepcion   time.Time `bson:"recepcion,omitempty"`
	Salon       Salon     `bson:"salon,omitempty"`
	LstCheckins []Checkin `bson:"lstCheckins,omitempty"`
	Checkins    []string  `bson:"checkins,omitempty"`
	Origin      string    `bson:"origin,omitempty"`
	Class       string    `bson:"_class,omitempty"`
}

var wg sync.WaitGroup

func main() {
	// Conexión a MongoDB
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.TODO())

	// Verificamos la conexión
	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Conectado a MongoDB")

	// Selección de la base de datos y colecciones
	db := client.Database("server_svip")
	cierreCollection := db.Collection("cierre")
	checkinCollection := db.Collection("checkins")

	// Procesar todos los cierres
	cursor, err := cierreCollection.Find(context.TODO(), bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.TODO())

	// Contador para las goroutines
	const numGoroutines = 20
	semaphore := make(chan struct{}, numGoroutines)

	for cursor.Next(context.TODO()) {
		var cierre Cierre
		if err := cursor.Decode(&cierre); err == nil {
			// log.Fatal(err)
			// Utilizamos una goroutine para cada cierre
			semaphore <- struct{}{} // Bloquea cuando alcanzamos el límite de goroutines
			wg.Add(1)
			go func(cierre Cierre) {
				defer wg.Done()
				defer func() { <-semaphore }() // Libera la goroutine al finalizar

				processCierre(cierreCollection, checkinCollection, cierre)
			}(cierre)
		}

	}

	wg.Wait() // Esperar que todas las goroutines finalicen
	fmt.Println("Proceso completado")
}

// Función para procesar cada cierre
func processCierre(cierreCollection, checkinCollection *mongo.Collection, cierre Cierre) {
	log.Printf("Procesando cierre: %s", cierre.ID)

	var checkinIDs []string

	// Mover checkins a la colección correspondiente y generar referencias
	for _, checkin := range cierre.LstCheckins {
		// year := checkin.FechaIngreso.Year()
		// collectionName := fmt.Sprintf("checkins_%d", year)

		// // Insertar el checkin en la colección correspondiente
		// _, err := checkinCollection.Database().Collection(collectionName).InsertOne(context.TODO(), checkin)
		// if err != nil {
		// 	log.Printf("Error insertando checkin %s: %v", checkin.ID, err)
		// 	continue
		// }

		checkinIDs = append(checkinIDs, checkin.ID)
		// log.Printf("Checkin %s movido a la colección %s", checkin.ID, collectionName)
	}

	// Actualizar el cierre con las referencias a los checkins
	year := cierre.Envio.Year()
	collectionName := fmt.Sprintf("cierres_%d", year)
	_, err := cierreCollection.Database().Collection(collectionName).InsertOne(
		context.TODO(),
		bson.M{
			"_id":       cierre.ID,
			"checkins":  checkinIDs,
			"envio":     cierre.Envio,
			"recepcion": cierre.Recepcion,
			"salon":     cierre.Salon,
			"origin":    cierre.Origin,
			"_class":    cierre.Class,
		},
	)
	if err != nil {
		log.Printf("Error actualizando cierre %s: %v", cierre.ID, err)
	} else {
		log.Printf("Cierre %s actualizado con referencias a los checkins", cierre.ID)
	}
}
