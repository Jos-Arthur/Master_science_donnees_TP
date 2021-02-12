// commande pour importer les données json

mongoimport --db sd2 --collection restaurants /home/jose-arthur/Bureau/TP_SD/restaurants/restaurants.json 

Chaque restaurant a un nom, un quartier (borough), le type de cuisine, une adresse 
(document imbriqué, avec coordonnées GPS, rue et code postale), 
et un ensemble de notes (résultats des inspections).



db.getCollection('restaurants').find({})
db.restaurants.findOne()			


//Filtrage et comptage

db.restaurants.find( { "borough" : "Brooklyn" } ).count()

db.restaurants.find(
    { "borough" : "Brooklyn",
      "cuisine" : "Italian" }
).count()





//Filtrage incluant des documents imbriqués      
db.restaurants.find(
    { "borough" : "Brooklyn",
      "cuisine" : "Italian",
      "address.street" : "5 Avenue" }
)

/*Filtrage dans le but de rechercher un terme, dans cet exemple pizza. le 
 le i pour insensible à la casse     
*/

db.restaurants.find(
    { "borough" : "Brooklyn",
      "cuisine" : "Italian",
      "address.street" : "5 Avenue",
      "name" : /pizza/i }
)


      
/*PROJECTION :Filtrage avec choix des clés à retourner, implique un 2é parametre pour la
     pour la fonction find() */

db.restaurants.find(
    { "borough" : "Brooklyn",
      "cuisine" : "Italian",
      "address.street" : "5 Avenue",
      "name" : /pizza/i },
      {"name":1}
)

db.restaurants.find(
    { "borough" : "Brooklyn",
      "cuisine" : "Italian",
      "address.street" : "5 Avenue",
      "name" : /pizza/i },
      {"name":1, "_id":0}
)

//Filtrage avec operateurs
db.getCollection('restaurants').find(
    {"borough":"Manhattan",
     "grades.score":{$lt : 10, $not:{$gte:10}}
    },
    {"name":1,"grades.score":1, "_id":0}
    )
   
   
//Rechercher le plus recent element dont le grade est C, (Precision de l'indice 0) 
db.restaurants.find({
"grades.0.grade":"C"
},
{"name":1, "borough":1, "_id":0}
)
    

/*Utilisation de la fonction distinct, pour les afficher les valeurs sans doublons,
Cette requetes donne les differents quartiers */

db.restaurants.distinct("borough")

/*


Utilisation de la fonction aggregate() qui permet de specifier les chaines d'operations

Le pipeline d'agrégation , la fonction de réduction de carte et les méthodes d'agrégation à usage unique .

{$match :{}} ==permet de filtrer le contenu d'une collection, premier parametre de find()
{$project :{}}  == permet de definir le format de sortie de la requete, second parametre de find()
*/


db.restaurants.aggregate( [
    { $match : {
        "grades.0.grade":"C"
    }},
    { $project : {
        "name":1, "borough":1, "_id":0
    }}
] )

/*L'interpreteur de mongoDB utilisant le langage javascript, possibilite de 
 creer des variables puis de les reultiliser
    */
    
varMatch = { $match : { "grades.0.grade":"C"} };
varProject = { $project : {"name":1, "borough":1, "_id":0}};
db.restaurants.aggregate( [ varMatch, varProject ] );

/* TRI:
Trions maintenant le résultat par nom de restaurant par ordre croissant 
(croissant : 1, décroissant : -1) :
*/

varSort = { $sort : {"name":1} };
db.restaurants.aggregate( [ varMatch, varProject, varSort ] );

/* GROUPEMENT SIMPLE
Comptons maintenant le nombre de ces restaurants (premier rang ayant pour valeur C). 
Pour cela, il faut définir un opérateur $group. Celui-ci doit contenir obligatoirement
une clé de groupement (_id), puis une clé (total) à laquelle on associe la fonction 
d'agrégation ($sum) :
*/


varGroup = { $group : {"_id" : null, "total" : {$sum : 1} } };
db.restaurants.aggregate( [ varMatch, varGroup ] );

/* GROUPEMENT PAR VALEUR
Comptons maintenant par quartier le nombre de ces restaurants. Il faut dans ce cas changer
la clé de groupement en y mettant la valeur de la clé "borough". Mais si l’on essaye ceci :
*/
varGroup2 = { $group : {"_id" : "borough", "total" : {$sum : 1} } };
db.restaurants.aggregate( [ varMatch, varGroup2 ] );

 
//Mise )à jour des donnees
/*UPDATE
Ajouter un commentaire sur un restaurant (opération $set)
*/
db.restaurants.update (
   {"_id" : ObjectId("594b9172c96c61e672dcd689")},
   {$set : {"comment" : "My new comment"}}
);


db.restaurants.update (
   {"grades.grade" : {$not : {$eq : "C"}}},
   {$set : {"comment" : "acceptable"}}
);
   
//REMOVE ==Supprimer
   /*
   Suppression tous les restaurants qui ont une note de 0.
   */
   
db.restaurants.remove(
{"note":0},
{"multi" : true}
);



/*Map-Reduce*/


db.villes.insertMany(
   [
		{
		city:"Los angeles",
		pop:51841,
		state:"CA"
		},
		{
		city:"Nashville",
		pop:1579,
		state:"TN"
		},
		{
		city:"Nashville",
		pop:612,
		state:"TN"
		},
		{
		city:"Memphis",
		pop:4144,
		state:"TN"
		},
		{
		city:"New york",
		pop:18913,
		state:"NY"
		},
		{
		city:"Nashville",
		pop:162,
		state:"TN"
		},
		{
		city:"Memphis",
		pop:1579,
		state:"TN"
		},

		{
		city:"New york",
		pop:1574,
		state:"TN"
		}
	]
)



db.villes.mapReduce(
	function(){ emit(this.city, this.pop);},
	function(key, values){ return Array.sum(values)},
	{
		query:{state:"TN"},
		out:"pop_total"
	}
);

db.pop_total.find().pretty()

